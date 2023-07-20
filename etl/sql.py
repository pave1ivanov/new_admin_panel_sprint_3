class SQL:
    @staticmethod
    def enrich_film_works():
        return """SELECT
                      fw.id as fw_id, 
                      fw.title, 
                      fw.description, 
                      fw.rating, 
                      fw.type, 
                      fw.created, 
                      fw.modified, 
                      pfw.role, 
                      p.id, 
                      p.full_name,
                      g.name
                  FROM content.film_work fw
                  LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                  LEFT JOIN content.person p ON p.id = pfw.person_id
                  LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                  LEFT JOIN content.genre g ON g.id = gfw.genre_id
                  WHERE fw.id IN %s;"""

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
