import os
import subprocess

from .common.utils.packages import get_alink_lib_path, get_pyflink_path, get_alink_plugins_path
from .py4j_util import get_java_class


def _get_default_local_ip():
    cmd = '''ifconfig eth0|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"'''
    local_ip = None
    try:
        local_ip = subprocess.check_output(cmd, shell=True, stderr=subprocess.DEVNULL).decode("utf-8").strip()
    except subprocess.CalledProcessError:
        pass
    if local_ip is None or local_ip == '':
        local_ip = "localhost"
    return local_ip


_env_variables = [
    ("debug_mode", "ENABLE_DEBUG_MODE", False, bool),
    ("flink_home", "FLINK_HOME", get_pyflink_path),
    ("alink_deps_dir", "ALINK_DEPS_DIR", get_alink_lib_path),
    ("alink_plugins_dir", "ALINK_PLUGINS_DIR", get_alink_plugins_path),
    ("local_ip", "LOCAL_IP", _get_default_local_ip),
    ("gateway_port", "PYALINK_GATEWAY_PORT", None),
    ("java_opts_str", "PYALINK_JAVA_OPTS", None)
]

g_config = {}

for entry in _env_variables:
    if entry[1] in os.environ:
        env_v = os.getenv(entry[1])
    else:
        env_v = entry[2]() if callable(entry[2]) else entry[2]
    if len(entry) > 3:
        env_v = entry[3](env_v)
    g_config[entry[0]] = env_v

g_config['java_opts'] = g_config['java_opts_str'].split() if g_config['java_opts_str'] is not None else []


class AlinkGlobalConfiguration:

    @classmethod
    def _get_cls(cls):
        _j_alink_global_configuration = get_java_class("com.alibaba.alink.common.AlinkGlobalConfiguration")
        return _j_alink_global_configuration

    @classmethod
    def isPrintProcessInfo(cls):
        return cls._get_cls().isPrintProcessInfo()

    @classmethod
    def setPrintProcessInfo(cls, printProcessInfo):
        return cls._get_cls().setPrintProcessInfo(printProcessInfo)

    @classmethod
    def setPluginDir(cls, pluginDir):
        return cls._get_cls().setPluginDir(pluginDir)

    @classmethod
    def getPluginDir(cls):
        return cls._get_cls().getPluginDir()

    @classmethod
    def setAutoPluginDownload(cls, autoPluginDownload):
        return cls._get_cls().setAutoPluginDownload(autoPluginDownload)

    @classmethod
    def getAutoPluginDownload(cls):
        return cls._get_cls().getAutoPluginDownload()

    @classmethod
    def setPluginUrl(cls, url):
        return cls._get_cls().setPluginUrl(url)

    @classmethod
    def getPluginUrl(cls):
        return cls._get_cls().getPluginUrl()

    @classmethod
    def getFlinkVersion(cls):
        return cls._get_cls().getFlinkVersion()

    @classmethod
    def getPluginDownloader(cls):
        from .plugin_downloader import PluginDownloader
        return PluginDownloader()
