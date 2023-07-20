import os
import backoff
import psycopg2
from time import sleep
from logger import logger
from typing import Coroutine
from datetime import datetime
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, ConnectionError

from sql import SQL
from decorators import coroutine
from state.models import State, FilmWork, Person
from state.json_file_storage import JsonFileStorage
from es_index import mapping


dotenv_path = os.path.abspath(os.path.dirname(__file__) + '/../config/.env')
load_dotenv(dotenv_path)
INDEX = 'film_work'

TABLE_NAMES = [
    'genre',
    'person',
    'film_work'
]

def _id_separator(results: list[str, datetime]) -> tuple[list, str]:
    """Separate list of ids and max datetime"""
    ids = []
    modified = []
    for result in results:
        ids.append(result[0])
        modified.append(result[1])
    last_modified = max(modified)
    return ids, last_modified


@backoff.on_exception(backoff.expo,
                      psycopg2.OperationalError,
                      logger=logger)
@coroutine
def extract_changed_from(cursor, next_node: Coroutine) -> Coroutine[tuple[str, str], None, None]:
    """Collect ids of modified rows from a given table"""
    while True:
        table_name, last_modified = (yield)
        logger.info(f'Looking for changed data for indexing')
        logger.info(f'Fetching rows from "{table_name}" changed after {last_modified}')
        cursor.execute(SQL.select_modified_ids(table_name), (last_modified,))
        while results := cursor.fetchmany(size=500):
            logger.info(f'Fetching {len(results)} rows from "{table_name}" changed after {last_modified}')
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
            continue

        logger.info(f'Fetching film works related to rows fetched from {table_name}')
        sql = SQL.select_film_works_from(table_name)
        cursor.execute(sql, (tuple(ids),))
        while results := cursor.fetchmany(size=500):
            film_work_ids, last_modified_film_works = _id_separator(results)
            next_node.send((film_work_ids, last_modified_film_works))

        logger.info(f'Updating state: {table_name} - {last_modified}')
        state.set_state(table_name, last_modified)


@backoff.on_exception(backoff.expo,
                      psycopg2.OperationalError,
                      logger=logger)
@coroutine
def enrich_film_work(cursor, next_node: Coroutine) -> Coroutine[tuple[list, str], None, None]:
    """ Enrich given film work ids with all data available """
    while True:
        film_work_ids, last_modified = (yield)
        logger.info(f'Enriching film works')
        sql = SQL.enrich_film_works()
        cursor.execute(sql, (tuple(film_work_ids),))
        results = cursor.fetchall()
        next_node.send((results, last_modified))


@coroutine
def transform_movies(next_node: Coroutine) -> Coroutine[tuple[list, str], None, None]:
    """ Transform film work Postgres entities to the Elasticsearch index format """
    while True:
        sql_results, last_modified = (yield)
        logger.info(f'Transforming film work data')

        film_works = {}
        for result in sql_results:
            role = result[7]
            person = Person(
                id=result[8],
                name=result[9],
            )
            film_work = FilmWork(
                id=result[0],
                title=result[1],
                description=result[2],
                imdb_rating=result[3],
                genres=result[10],
            )
            if film_work.id not in film_works:
                film_works[film_work.id] = film_work

            fw = film_works[film_work.id]
            if role == 'director':
                fw.director += person.name + ' '
            elif role == 'actor':
                fw.actors_names += person.name + ' '
                fw.actors.append(person)
            elif role == 'writer':
                fw.writers_names += person.name + ' '
                fw.writers.append(person)

        next_node.send((film_works.values(), last_modified))


@backoff.on_exception(backoff.expo,
                      ConnectionError,
                      logger=logger)
@coroutine
def load_movies(es: Elasticsearch, state: State) -> Coroutine[tuple[list[FilmWork], str], None, None]:
    """ Load information about changed film works to Elasticsearch """
    while True:
        film_works, last_modified = (yield)
        logger.info(f'Received for loading {len(film_works)} film works')
        for film_work in film_works:
            es.index(
                index=INDEX,
                id=film_work.id,
                document=film_work.json(),
            )

        logger.info(f'Loading to Elasticsearch complete')
        logger.info(f'Updating state: film_work - {last_modified}')
        state.set_state('film_work', last_modified)


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
    logger.info(f'Attempted to create Elasticsearch index. Response: {response}')

    with psycopg2.connect(**dsn) as conn, conn.cursor() as cur:
        loader_coro = load_movies(es, state)
        transformer_coro = transform_movies(loader_coro)
        enricher_coro = enrich_film_work(cur, transformer_coro)
        film_work_ids_extractor_coro = extract_film_works_from_changed(cur, enricher_coro)
        extractor_coro = extract_changed_from(cur, film_work_ids_extractor_coro)

        logger.info('Starting ETL process for updates ...')
        while True:
            for table_name in TABLE_NAMES:
                extractor_coro.send((table_name, state.get_state(table_name) or str(datetime.min)))
            sleep(5)
