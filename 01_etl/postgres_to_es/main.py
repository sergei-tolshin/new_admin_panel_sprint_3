import logging
from pathlib import Path

from config.settings import etl_settings
from services.etl import JsonFileStorage, State
from services.etl.service import ETL

"""Настройка логирования"""
_log_format = ("%(asctime)s - [%(levelname)s] - %(name)s "
               "(%(filename)s).%(funcName)s(%(lineno)d) > %(message)s")
logging.basicConfig(filename='logs.log',
                    level=logging.INFO,
                    format=_log_format,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

"""Хранилище состояний"""
default_file_path: str = f'{Path(__file__).resolve().parent}'
storage = JsonFileStorage(file_path=default_file_path,
                          file_name=etl_settings.STATE_FILE_NAME)


def load_to_es():
    etl_state = State(storage=storage)

    if etl_state.get_state('etl_process') == 'started':
        logger.error('ETL process already started, please stop it before run!')
        return

    while True:
        with ETL(state=State(storage=storage)) as etl:
            logger.info('Start extract data from PostgreSQL')
            number_data, modified_data = etl.extract()

            logger.info('Extracted %d modified data', number_data)

            if modified_data is not None:
                transformed_data = etl.transform(modified_data=modified_data)

                logger.info('Start data transfer to Elasticsearch')
                etl.load(transformed_data=transformed_data)

                logger.info('Save state of data modified')
                etl.save_state()
            else:
                logger.info('No data to load into Elasticsearch')


if __name__ == '__main__':
    try:
        load_to_es()
    except KeyboardInterrupt:
        logger.info('ETL process interrupted')
