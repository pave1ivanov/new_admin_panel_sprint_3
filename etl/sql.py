class SQL:
    @staticmethod
    def enrich_film_works():
        return """SELECT
                   fw.id,
                   fw.title,
                   fw.description,
                   fw.rating,
                   fw.type,
                   fw.created,
                   fw.modified,
                   COALESCE (
                       json_agg(
                           DISTINCT jsonb_build_object(
                               'role', pfw.role,
                               'id', p.id,
                               'name', p.full_name
                           )
                       ) FILTER (WHERE p.id is not null),
                       '[]'
                   ) as persons,
                   COALESCE (
                       json_agg(
                           DISTINCT jsonb_build_object(
                               'id', g.id,
                               'name', g.name
                           )
                       ) FILTER (WHERE g.id is not null),
                       '[]'
                   ) as genres
                FROM content.film_work fw
                LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                LEFT JOIN content.person p ON p.id = pfw.person_id
                LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                LEFT JOIN content.genre g ON g.id = gfw.genre_id
                WHERE fw.id IN %s
                GROUP BY fw.id
                ORDER BY fw.modified;"""

    @staticmethod
    def select_modified_ids(table_name, limit=1000):
        return f"""SELECT id, modified
                   FROM content.{table_name}
                   WHERE modified > %s
                   ORDER BY modified
                   LIMIT {limit}; """


    @staticmethod
    def select_film_works_from(table_name, limit=1000):
        return f"""SELECT fw.id, fw.modified
                   FROM content.film_work fw
                   LEFT JOIN content.{table_name}_film_work ON {table_name}_film_work.film_work_id = fw.id
                   WHERE {table_name}_film_work.{table_name}_id IN %s
                   ORDER BY fw.modified
                   LIMIT {limit};"""
