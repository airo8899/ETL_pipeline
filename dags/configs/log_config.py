import logging
from copy import deepcopy

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.configuration import conf


class FilterTrash(logging.Filter):
    """Filter task logs"""

    def filter(self, record) -> bool:
        if record.filename in ['taskinstance.py', 'standard_task_runner.py', 'logging_mixin.py', 'local_task_job.py'] or 'Tmp dir root location' in record.msg or 'Running command:' in record.msg:
            return False
        else:
            return True

LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)

LOG_LEVEL_TASK: str = conf.get('logging', 'LOGGING_LEVEL_TASK').upper()
SIMPLE_LOG_FORMAT: str = conf.get('logging', 'SIMPLE_LOG_FORMAT')

LOGGING_CONFIG['loggers']['airflow.task']['level'] = LOG_LEVEL_TASK

LOGGING_CONFIG['formatters']['airflow_simple'] = {'format': SIMPLE_LOG_FORMAT}
LOGGING_CONFIG['handlers']['task']['formatter'] = 'airflow_simple'

LOGGING_CONFIG['filters']['filter_other'] = {'()': 'configs.log_config.FilterTrash'}
LOGGING_CONFIG['handlers']['task']['filters'] = ['mask_secrets', 'filter_other']
LOGGING_CONFIG['loggers']['airflow.task']['filters'] = ['mask_secrets', 'filter_other']