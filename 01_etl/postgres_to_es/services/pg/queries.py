from config.settings import etl_settings


def modified_filmworks(timestamp) -> str:
    """Запрос получения обновленных фильмов за промежуток времени"""
    return f"""
        SELECT id, modified
        FROM content.film_work
        WHERE modified > '{timestamp}'
        ORDER BY modified
        LIMIT {etl_settings.LIMIT};
    """


def modified_person(timestamp) -> str:
    """Запрос получения обновленных персонажей за промежуток времени"""
    return f"""
        SELECT id, modified
        FROM content.person
        WHERE modified > '{timestamp}'
        ORDER BY modified
        LIMIT {etl_settings.LIMIT};
    """


def filmwork_by_person(persons: list) -> str:
    """
    Запрос получения фильмов, в которых приняли
    участие обновленные персонажи
    """
    condition = f"IN {tuple(persons)}" if len(
        persons) > 1 else f"= '{persons[0]}'"
    return f"""
        SELECT fw.id, fw.modified
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        WHERE pfw.person_id {condition}
        ORDER BY fw.modified;
    """


def modified_genre(timestamp) -> str:
    """Запрос получения обновленных жанров за промежуток времени"""
    return f"""
        SELECT id, modified
        FROM content.genre
        WHERE modified > '{timestamp}'
        ORDER BY modified
        LIMIT {etl_settings.LIMIT};
    """


def filmwork_by_genre(genres: list) -> str:
    """Запрос получения фильмов по обновленным жанрам"""
    condition = f"IN {tuple(genres)}" if len(
        genres) > 1 else f"= '{genres[0]}'"
    return f"""
        SELECT fw.id, fw.modified
        FROM content.film_work fw
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        WHERE gfw.genre_id {condition}
        ORDER BY fw.modified;
    """


def filmwork_by_id(ids: tuple) -> str:
    """Запрос получения всей информации экземпляров фильмов"""
    condition = f"IN {tuple(ids)}" if len(ids) > 1 else f"= '{ids[0]}'"
    return f"""
        SELECT
            fw.id as fw_id,
            fw.title,
            fw.description,
            fw.rating,
            fw.type,
            fw.created,
            fw.modified,
            pfw.role,
            p.id as person_id,
            p.full_name,
            g.name as genre
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id {condition};
    """
