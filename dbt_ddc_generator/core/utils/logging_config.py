import logging.config
import os


def setup_logging(default_level: int = logging.INFO, env_key: str = "LOG_CFG") -> None:
    """
    Setup logging configuration.

    Args:
        default_level: Default logging level
        env_key: Environment variable that points to logging config file
    """
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "standard",
                "stream": "ext://sys.stdout",
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "DEBUG",
                "formatter": "standard",
                "filename": "dbt_ddc_generator.log",
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5,
            },
        },
        "loggers": {
            "": {  # root logger
                "handlers": ["console", "file"],
                "level": "INFO",
                "propagate": True,
            },
            "dbt_ddc_generator": {
                "handlers": ["console", "file"],
                "level": "DEBUG",
                "propagate": False,
            },
        },
    }

    path = os.getenv(env_key, None)
    if path and os.path.exists(path):
        with open(path, "rt") as f:
            try:
                config = eval(f.read())
            except Exception as e:
                print(f"Error loading logging configuration: {e}")

    logging.config.dictConfig(config)
