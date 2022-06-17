import os
import sys
from threading import RLock
from typing import Optional, List

from py4j.java_gateway import GatewayParameters, CallbackServerParameters, is_instance_of, JavaGateway, launch_gateway
from py4j.protocol import Py4JNetworkError

from .common.utils.packages import in_ipython

_gateway: Optional[JavaGateway] = None
_lock = RLock()


def set_java_gateway(gateway: JavaGateway):
    global _gateway
    global _lock
    with _lock:
        _gateway = gateway


def check_java_gateway_alive(gateway: JavaGateway) -> bool:
    if gateway is None:
        return False
    try:
        gateway.jvm.System.getProperty("java.runtime.name")
        return True
    except Exception or Py4JNetworkError:
        return False


def get_java_gateway():
    global _gateway
    global _lock
    with _lock:
        if not check_java_gateway_alive(_gateway):
            LocalJvmBridge.inst().close()
            _gateway = None
        if _gateway is None:
            _gateway = LocalJvmBridge.inst().gateway
    return _gateway


def get_java_class(name: str):
    return get_java_gateway().jvm.__getattr__(name)


def is_java_instance(java_object, java_class):
    return is_instance_of(get_java_gateway(), java_object, java_class)


def list_all_jars() -> List[str]:
    # noinspection PyProtectedMember
    from .config import g_config
    alink_deps_dir = g_config["alink_deps_dir"]
    flink_home = g_config["flink_home"]

    ret = []
    ret += [os.path.join(alink_deps_dir, x) for x in
            os.listdir(alink_deps_dir) if x.endswith('.jar')]
    ret += [os.path.join(flink_home, 'lib', x) for x in
            os.listdir(os.path.join(flink_home, 'lib'))
            if x.endswith('.jar')]

    ret += [os.path.join(flink_home, 'opt', x)
            for x in os.listdir(os.path.join(flink_home, 'opt'))
            if x.endswith('.jar') and x.startswith("flink-python")]
    return ret


class LocalJvmBridge(object):
    _bridge = None

    def __init__(self):
        self.process = None
        self.gateway = None
        self.app = None
        self.port = 0

    @classmethod
    def inst(cls):
        if cls._bridge is None:
            cls._bridge = LocalJvmBridge()
            cls._bridge.init()
        return cls._bridge

    def init(self):
        # noinspection PyProtectedMember
        from .config import g_config
        debug_mode = g_config["debug_mode"]

        # redirect stdout if in debug_mode
        # Note: if enableLazyXXX is used in Pipeline stages, Java side will print stuff.
        # This case is very difficult to handle, so do not set redirect_stdout to True.
        redirect_stdout = sys.stdout if debug_mode else None
        # redirect stderr if in debug_mode and not in IPython (will fail)
        redirect_stderr = sys.stderr if debug_mode and not in_ipython() else None

        try:
            # noinspection PyUnresolvedReferences
            from pyflink.pyflink_gateway_server import construct_log_settings
            if os.name != 'nt':
                try:
                    from pyflink.pyflink_gateway_server import prepare_environment_variables
                    env = dict(os.environ)
                    prepare_environment_variables(env)
                    log_settings = construct_log_settings(env)
                except TypeError:
                    # noinspection PyArgumentList
                    log_settings = construct_log_settings()
            else:
                log_settings = []
        except ImportError:
            log_settings = []

        javaopts = g_config['java_opts'].copy()
        javaopts += log_settings
        if os.name == 'nt':
            # Limit Java side to only detect and use Java implementation of BLAS/LAPACK/ARPACK.
            # Java process started through py4j will stuck when trying to load native libraries.
            # See https://github.com/bartdag/py4j/issues/444
            javaopts += [
                "-Dcom.github.fommil.netlib.BLAS=com.github.fommil.netlib.F2jBLAS",
                "-Dcom.github.fommil.netlib.LAPACK=com.github.fommil.netlib.F2jLAPACK",
                "-Dcom.github.fommil.netlib.ARPACK=com.github.fommil.netlib.F2jARPACK"
            ]

        if g_config['gateway_port'] is not None:
            self.port = int(g_config['gateway_port'])
        else:
            self.port = launch_gateway(
                port=0, javaopts=javaopts, die_on_exit=True, daemonize_redirect=True,
                redirect_stdout=redirect_stdout, redirect_stderr=redirect_stderr,
                classpath=os.pathsep.join(list_all_jars()))
        print('JVM listening on 127.0.0.1:{}'.format(self.port))
        self.gateway = JavaGateway(
            gateway_parameters=GatewayParameters(port=self.port, auto_field=True, auto_convert=True),
            callback_server_parameters=CallbackServerParameters(port=0, daemonize=True, daemonize_connections=True),
            start_callback_server=False)

        callback_server_port = self.gateway.get_callback_server().get_listening_port()
        self.gateway.java_gateway_server.resetCallbackClient(
            self.gateway.java_gateway_server.getCallbackClient().getAddress(),
            callback_server_port)

    def close(self):
        self.gateway.close(keep_callback_server=True)
        self._bridge = None
