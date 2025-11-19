from enum import Enum

# Env related ENUM
class EnvEnum(Enum):
    DATABRICKS_ENV = "databricks"
    LOCAL_ENV = "local"


# Pylogger related ENUM
class PyLogTypeEnum(Enum):
    LOG_INFO = "info"
    LOG_ERROR = "error"
    LOG_WARNING = "warning"
    LOG_DEBUG = "debug"