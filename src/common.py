from logging import (
    getLogger,
    StreamHandler,
    DEBUG,
    WARNING,
    INFO
)
import sys

# INFO以下のログを標準出力する
stdout_handler = StreamHandler(stream=sys.stdout)
stdout_handler.setLevel(DEBUG)
stdout_handler.addFilter(lambda record: record.levelno <= INFO)

# WARNING以上のログを標準エラー出力する
stderr_handler = StreamHandler(stream=sys.stderr)
stderr_handler.setLevel(WARNING)

logger = getLogger(__name__)
logger.setLevel(DEBUG)
logger.addHandler(stdout_handler)
logger.addHandler(stderr_handler)


def logging(func):
    def wrapper(*args, **kwargs):
        logger.info(func.__name__)
        logger.info('start')
        result = func(*args, **kwargs)
        logger.info('done')
        return result
    return wrapper
