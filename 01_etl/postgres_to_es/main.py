import logging
from time import sleep

from config.settings import etl_settings
from services.etl import etl_state
from services.etl.service import ETL

_log_format = ("%(asctime)s - [%(levelname)s] - %(name)s "
               "(%(filename)s).%(funcName)s(%(lineno)d) > %(message)s")
logging.basicConfig(filename='logs.log',
                    level=logging.INFO,
                    format=_log_format,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def load_to_es():
    if etl_state.get_state('etl_process') == 'started':
        logger.warning(
            'ETL process already started, please stop it before run!')
        return
    else:
        etl_state.set_state('etl_process', 'started')
        logger.info('ETL process started')

    while True:
        etl = ETL()

        logger.info('Start extract data from PostgreSQL')
        number_data, modified_data = etl.extract()

        logger.info(f'Extracted {number_data} modified data')

        if modified_data is not None:
            transformed_data = etl.transform(modified_data=modified_data)

            logger.info('Start data transfer to Elasticsearch')
            etl.load(transformed_data=transformed_data)

            logger.info('Save state of data modified')
            etl.save_state()
        else:
            logger.info('No data to load into Elasticsearch')

        logger.info('Close all connections ...')
        etl.close_connect()

        logger.info(f'Pause {etl_settings.UPLOAD_INTERVAL} seconds')
        sleep(etl_settings.UPLOAD_INTERVAL)


if __name__ == '__main__':
    try:
        load_to_es()
    except KeyboardInterrupt:
        etl_state.set_state('etl_process', 'stopped')
        logger.info('ETL process stopped')
    except Exception as e:
        etl_state.set_state('etl_process', 'stopped')
        logger.info(f'ETL process stopped: {e}')
