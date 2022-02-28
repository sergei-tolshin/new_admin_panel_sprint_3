import logging
from functools import wraps
from time import sleep
from typing import Type

logger = logging.getLogger(__name__)


def backoff(
    exception: Type[Exception],
    start_sleep_time=0.1,
    factor=2,
    border_sleep_time=30,
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
    :return: результат выполнения функции
    """
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            attempt = 0
            time_out = start_sleep_time
            while True:
                logger.info(f'Try number: {attempt + 1} - delay {time_out}')
                try:
                    attempt += 1
                    connection = func(*args, **kwargs)
                    return connection
                except exception as e:
                    logger.exception(
                        f'Error message: {e}\n'
                        f'Wait for {time_out} seconds and try again'
                    )
                    if time_out >= border_sleep_time:
                        time_out = border_sleep_time
                    else:
                        time_out += start_sleep_time * 2 ** factor
                    sleep(time_out)
        return inner
    return func_wrapper
