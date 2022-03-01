import logging
from functools import wraps
from time import sleep

logger = logging.getLogger(__name__)


def backoff(
    exception: list,
    start_sleep_time=0.1,
    factor=2,
    border_sleep_time=30,
    max_attempts=100
):
    """
    Функция для повторного выполнения функции через некоторое время,
    если возникла ошибка. Использует наивный экспоненциальный рост времени
    повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param exception: исключение, которое отлавливается
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :param max_attempts: максимальное количество попыток подключения
    :return: результат выполнения функции
    """
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            attempt = 0
            time_out = start_sleep_time
            while True:
                try:
                    attempt += 1
                    logger.info('Attempt %d out of %d - delay %s',
                                attempt, max_attempts, time_out)
                    connection = func(*args, **kwargs)
                    return connection
                except exception as error:
                    logger.error('Error message: %s', error)

                    """Вызываем исключение при привышении количества попыток"""
                    if attempt == max_attempts:
                        raise ConnectionError

                    if time_out >= border_sleep_time:
                        time_out = border_sleep_time
                    else:
                        time_out += start_sleep_time * 2 ** factor

                    logger.warning(
                        'Wait for %s seconds and try again', time_out)
                    sleep(time_out)

        return inner
    return func_wrapper
