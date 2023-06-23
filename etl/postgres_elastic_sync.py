import psycopg2
from time import sleep
from logger import logger
from typing import Coroutine
from datetime import datetime

from decorators import coroutine
from state.models import Movie, State
from state.json_file_storage import JsonFileStorage

STATE_KEY = 'last_movies_updated'


@coroutine
def extract_changed_movies(cursor, next_node: Coroutine) -> Coroutine[datetime, None, None]:
    while last_updated := (yield):
        logger.info(f'Fetching movies changed after {last_updated}')
        sql = 'SELECT * FROM movies WHERE updated_at > %s ORDER BY updated_at ASC'
        cursor.execute(sql, (last_updated,))
        while results := cursor.fetchmany(size=1000):
            next_node.send(results)


# todo: add backoff
@coroutine
def transform_movies(next_node: Coroutine) -> Coroutine[list[dict], None, None]:
    while movie_dicts := (yield):
        batch = []
        for movie_dict in movie_dicts:
            movie = Movie(**movie_dict)
            movie.title = movie.title.upper()
            logger.info(movie.json())
            batch.append(movie)
        next_node.send(batch)


# todo: add backoff
@coroutine
def load_movies(state: State) -> Coroutine[list[Movie], None, None]:
    while movies := (yield):
        logger.info(f'Received for loading {len(movies)} movies')
        # TODO: load to ElasticSearch
        state.set_state(STATE_KEY, str(movies[-1].updated_at))


if __name__ == '__main__':
    state = State(JsonFileStorage(logger=logger))

    dsn = 'todo'  # TODO: load dict with DSN from .env
    print(dsn)

    with psycopg2.connect(**dsn) as conn, conn.cursor('cur') as cur:
        loader_coro = load_movies(state)
        transformer_coro = transform_movies(next_node=loader_coro)
        extractor_coro = extract_changed_movies(cur, transformer_coro)

        while True:
            last_movies_updated = state.get_state(STATE_KEY)
            logger.info('Starting ETL process for updates ...')

            extractor_coro.send(last_movies_updated or str(datetime.min))

            sleep(10)
