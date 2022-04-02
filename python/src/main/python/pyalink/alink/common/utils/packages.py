import os


def is_flink_1_9() -> bool:
    return False


def get_alink_lib_path() -> str:
    import pyalink
    import os
    for path in pyalink.__path__:
        lib_path = os.path.join(path, "lib")
        if os.path.isdir(lib_path):
            return lib_path
    raise Exception("Cannot find pyalink Java libraries, please check your installation.")


def get_pyflink_path() -> str:
    import pyflink
    for path in pyflink.__path__:
        if os.path.isdir(path):
            return path
    print("Warning: cannot find pyflink path. "
          "If not running using 'flink run', please check if PyFlink is installed correctly.")


def in_ipython() -> bool:
    """
    Test if in ipython
    :return: in ipython or not
    :rtype bool
    """
    from IPython import get_ipython
    return get_ipython() is not None


def get_alink_plugins_path():
    alink_lib_path = get_alink_lib_path()
    return os.path.join(alink_lib_path, "plugins")
