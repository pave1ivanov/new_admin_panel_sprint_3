import os
import json
import backoff
import psycopg2
from time import sleep
from logger import logger
from typing import Coroutine
from datetime import datetime
from dotenv import load_dotenv
from contextlib import closing
from elasticsearch import Elasticsearch, ConnectionError, helpers

from sql import SQL
from decorators import coroutine
from state.models import State, FilmWork, Person
from state.json_file_storage import JsonFileStorage
from es_index import mapping


dotenv_path = os.path.abspath(os.path.dirname(__file__) + '/../config/.env')
load_dotenv(dotenv_path)

INDEX = 'film_work'
TABLE_NAMES = (
    'genre',
    'person',
    'film_work'
)

def _id_separator(results: list[str, datetime]) -> tuple[list, str]:
    """Separate list of ids and max datetime"""
    ids = []
    modified = []
    for result in results:
        ids.append(result[0])
        modified.append(result[1])
    last_modified = max(modified)
    return ids, last_modified

def _actions_generator(film_works: list[FilmWork]):
    """Yields actions for elasticsearch bulk index helper"""
    for film_work in film_works:
        yield {
            '_index': INDEX,
            '_id': film_work.id,
            '_source': film_work.json(),
        }


@backoff.on_exception(backoff.expo,
                      psycopg2.OperationalError,
                      logger=logger)
@coroutine
def extract_changed_from(cursor, next_node: Coroutine) -> Coroutine[tuple[str, str], None, None]:
    """Collect ids of modified rows from a given table"""
    while True:
        table_name, last_modified = (yield)
        logger.info('Looking for changed data for indexing in %s', table_name)
        cursor.execute(SQL.select_modified_ids(table_name), (last_modified,))
        while results := cursor.fetchmany(size=500):
            logger.info('Fetching %s rows from %s changed after %s', len(results), table_name, last_modified)
            ids, last_modified = _id_separator(results)
            next_node.send((table_name, last_modified, ids))


@backoff.on_exception(backoff.expo,
                      psycopg2.OperationalError,
                      logger=logger)
@coroutine
def extract_film_works_from_changed(cursor, next_node: Coroutine) -> Coroutine[tuple[str, str, list], None, None]:
    """ Collect ids of film works corresponded to the modified rows """
    while True:
        table_name, last_modified, ids = (yield)

        if table_name == 'film_work':
            next_node.send((ids, last_modified))
            state.set_state(table_name, last_modified)
            continue

        logger.info('Fetching film works related to rows fetched from %s', table_name)
        sql = SQL.select_film_works_from(table_name)
        cursor.execute(sql, (tuple(ids),))
        while results := cursor.fetchmany(size=500):
            film_work_ids, last_modified_film_works = _id_separator(results)
            next_node.send((film_work_ids, last_modified_film_works))

        logger.info('Updating state: %s - %s', table_name, last_modified)
        state.set_state(table_name, last_modified)


@backoff.on_exception(backoff.expo,
                      psycopg2.OperationalError,
                      logger=logger)
@coroutine
def enrich_film_work(cursor, next_node: Coroutine) -> Coroutine[tuple[list, str], None, None]:
    """ Enrich given film work ids with all data available """
    while True:
        film_work_ids, last_modified = (yield)
        logger.info('Enriching film works')
        sql = SQL.enrich_film_works()
        cursor.execute(sql, (tuple(film_work_ids),))
        results = cursor.fetchall()
        next_node.send((results, last_modified))


@coroutine
def transform_movies(next_node: Coroutine) -> Coroutine[tuple[list, str], None, None]:
    """ Transform film work Postgres entities to the Elasticsearch index format """
    while True:
        sql_results, last_modified = (yield)
        logger.info('Transforming film work data')

        film_works = []

        for result in sql_results:
            film_work = FilmWork(
                id=result[0],
                title=result[1],
                description=result[2],
                imdb_rating=result[3],
                genres=' '.join(result[8]),
            )
            for person_dict in result[7]:
                person = Person(id=person_dict['id'], name=person_dict['name'])
                if person_dict['role'] == 'director':
                    film_work.director += person.name + ' '
                elif person_dict['role'] == 'actor':
                    film_work.actors_names += person.name + ' '
                    film_work.actors.append(person)
                elif person_dict['role'] == 'writer':
                    film_work.writers_names += person.name + ' '
                    film_work.writers.append(person)
            film_works.append(film_work)

        next_node.send(film_works)


@backoff.on_exception(backoff.expo,
                      ConnectionError,
                      logger=logger)
@coroutine
def load_movies(es: Elasticsearch) -> Coroutine[tuple[list[FilmWork], str], None, None]:
    """ Load information about changed film works to Elasticsearch """
    while True:
        film_works = (yield)
        logger.info('Received for loading %s film works', len(film_works))

        helpers.bulk(es, _actions_generator(film_works))
        logger.info('Loading to Elasticsearch complete')


if __name__ == '__main__':
    dsn = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST'),
        'port': os.environ.get('DB_PORT'),
        'options': '-c search_path=content',
    }
    es = Elasticsearch(f"http://{os.environ.get('ELASTIC_HOST')}:{os.environ.get('ELASTIC_PORT')}")
    state = State(JsonFileStorage(logger=logger))

    while not es.ping():
        logger.info('Waiting for Elasticsearch connection...')
        sleep(1)

    response = es.indices.create(
        index=INDEX,
        body=mapping,
        ignore=400,
    )
    logger.info('Attempted to create Elasticsearch index. Response: %s', response)

    with closing(psycopg2.connect(**dsn)) as conn, conn.cursor() as cur:
        loader_coro = load_movies(es)
        transformer_coro = transform_movies(loader_coro)
        enricher_coro = enrich_film_work(cur, transformer_coro)
        film_work_ids_extractor_coro = extract_film_works_from_changed(cur, enricher_coro)
        extractor_coro = extract_changed_from(cur, film_work_ids_extractor_coro)

        logger.info('Starting ETL process for updates ...')
        while True:
            for table_name in TABLE_NAMES:
                extractor_coro.send((table_name, state.get_state(table_name) or str(datetime.min)))
            sleep(int(os.environ.get('ETL_ITER_PAUSE_TIME')) or 5)
