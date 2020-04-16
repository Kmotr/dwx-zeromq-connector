import json
import logging.config
import os


def setup_logging(default_path='logging.json',
                  default_level=logging.INFO,
                  env_key='LOG_CFG'
                  ):
    """Setup logging configuration
    :param default_path:'logging.json'
    :param default_level:logging.INFO
    :param env_key:LOG_CFG

    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)
