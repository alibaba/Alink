from .batch import *
from .common.types import *
from .common.types.conversion.type_converters import collectToDataframes, dataframeToOperator
from .config import AlinkGlobalConfiguration
from .env import *
from .ipython_magic_command import *
from .pipeline import *
from .plugin_downloader import PluginDownloader
from .stream import *
from .udf import *

print("""
Use one of the following commands to start using PyAlink:
 - useLocalEnv(parallelism, flinkHome=None, config=None): run PyAlink scripts locally.
 - useRemoteEnv(host, port, parallelism, flinkHome=None, localIp="localhost", config=None): run PyAlink scripts on a Flink cluster.
 - getMLEnv(): run PyAlink scripts as PyFlink scripts, support 'flink run -py xxx.py'.
Call resetEnv() to reset environment and switch to another.
""")
