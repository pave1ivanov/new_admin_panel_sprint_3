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
from state.models import State, FilmWork, Person, Genre
from state.json_file_storage import JsonFileStorage
from es_index import get_index


dotenv_path = os.path.abspath(os.path.dirname(__file__) + '/../config/.env')
load_dotenv(dotenv_path)

INDICES = {
    Genre.__name__: 'genre',
    Person.__name__: 'person',
    FilmWork.__name__: 'film_work'
}

GENRE_TABLE_NAME = 'genre'
PERSON_TABLE_NAME = 'person'
FILM_WORK_TABLE_NAME = 'film_work'

TABLE_NAMES = (
    GENRE_TABLE_NAME,
    PERSON_TABLE_NAME,
    FILM_WORK_TABLE_NAME,
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

def _actions_generator(models: list):
    """Yields actions for elasticsearch bulk index helper"""
    index = INDICES[models[0].__class__.__name__]
    for model in models:
        yield {
            '_index': index,
            '_id': model.uuid,
            '_source': model.json(),
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
def enrich_genres(cursor, next_node: Coroutine) -> Coroutine[tuple[list, str], None, None]:
    """ Enrich given genres ids with all data available """
    while True:
        _, last_modified, genre_ids = (yield)
        logger.info('Enriching genres')
        sql = SQL.enrich_genres()
        cursor.execute(sql, (tuple(genre_ids),))
        results = cursor.fetchall()
        next_node.send((results, last_modified))


@coroutine
def transform_genres(next_node: Coroutine) -> Coroutine[tuple[list, str], None, None]:
    """ Transform genres Postgres entities to the Elasticsearch index format """
    while True:
        sql_results, last_modified = (yield)
        logger.info('Transforming genres data')

        genres = []

        for result in sql_results:
            genre = Genre(
                uuid=result[0],
                name=result[1],
                description=result[2],
            )
            genres.append(genre)

        genre_state.set_state(GENRE_TABLE_NAME, last_modified)
        next_node.send(genres)


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
                uuid=result[0],
                title=result[1],
                description=result[2],
                imdb_rating=result[3],
                genre=[Genre(uuid=genre['id'], name=genre['name']) for genre in result[8]],
            )
            for person_dict in result[7]:
                person = Person(uuid=person_dict['id'], full_name=person_dict['name'])
                if person_dict['role'] == 'director':
                    film_work.directors.append(person)
                elif person_dict['role'] == 'actor':
                    film_work.actors.append(person)
                elif person_dict['role'] == 'writer':
                    film_work.writers.append(person)
            film_works.append(film_work)

        next_node.send(film_works)


@backoff.on_exception(backoff.expo,
                      ConnectionError,
                      logger=logger)
@coroutine
def load_models(es: Elasticsearch) -> Coroutine[list, None, None]:
    """ Load information about changed models to Elasticsearch """
    while True:
        models = (yield)
        logger.info('Received for loading %s items of model %s', len(models), models[0].__class__.__name__)

        helpers.bulk(es, _actions_generator(models))
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
    state = State(JsonFileStorage(logger=logger, file_path='film_work_state.json'))
    genre_state = State(JsonFileStorage(logger=logger, file_path='genre_state.json'))

    while not es.ping():
        logger.info('Waiting for Elasticsearch connection...')
        sleep(1)

    for cls, name in INDICES.items():
        response = es.indices.create(
            index=name,
            body=get_index(cls),
            ignore=400,
        )
        logger.info('Attempted to create Elasticsearch index. Response: %s', response)

    with closing(psycopg2.connect(**dsn)) as conn, conn.cursor() as cur:
        loader_coro = load_models(es)

        # film work etl pipeline
        transformer_coro = transform_movies(loader_coro)
        enricher_coro = enrich_film_work(cur, transformer_coro)
        film_work_ids_extractor_coro = extract_film_works_from_changed(cur, enricher_coro)
        extractor_coro = extract_changed_from(cur, film_work_ids_extractor_coro)

        # genres etl pipeline
        transform_genres_coro = transform_genres(loader_coro)
        enrich_genres_coro = enrich_genres(cur, transform_genres_coro)
        genres_extractor_coro = extract_changed_from(cur, enrich_genres_coro)

        logger.info('Starting ETL process for updates ...')
        while True:
            # starting film work etl
            for table_name in TABLE_NAMES:
                extractor_coro.send((table_name, state.get_state(table_name) or str(datetime.min)))

            # starting genres etl
            genres_extractor_coro.send((GENRE_TABLE_NAME, genre_state.get_state(GENRE_TABLE_NAME) or str(datetime.min)))
            sleep(int(os.environ.get('ETL_ITER_PAUSE_TIME')) or 5)
