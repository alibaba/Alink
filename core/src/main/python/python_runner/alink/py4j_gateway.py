#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import sys
import time
from optparse import OptionParser
from typing import Optional

from py4j.java_gateway import JavaGateway, GatewayParameters, CallbackServerParameters


def get_class_from_name(cls_name):
    """
    Get Python class by the class name
    :param cls_name: class name
    :return: Python class
    """
    if '.' not in cls_name:
        if cls_name in globals():
            return globals()[cls_name]
        else:
            raise RuntimeError('cannot find class[{}]'.format(cls_name))
    else:
        module_name, cls_name = cls_name.rsplit('.', 1)
        import importlib
        # load the module, will raise ImportError if module cannot be loaded
        m = importlib.import_module(module_name)
        # get the class, will raise AttributeError if class cannot be found
        c = getattr(m, cls_name)
        return c


# noinspection PyMethodMayBeStatic
class PyMain(object):

    def __init__(self):
        self._cnt = 0
        self._running = True

    def check(self):
        sys.stdout.flush()
        return self._running

    def open(self, name):
        print('open from {}'.format(name), flush=True)
        self._cnt += 1
        return self._cnt

    def close(self, name):
        print('close from {}'.format(name), flush=True)
        self._cnt -= 1
        return self._cnt

    def shutdown(self, name):
        if self._cnt == 0:
            print('shutdown from {} .. OK'.format(name), flush=True)
            self._running = False
            return True
        else:
            print('shutdown from {} .. WAIT'.format(name), flush=True)
            return False

    def newobj(self, class_name):
        print('new object of class {}'.format(class_name), flush=True)
        cls = get_class_from_name(class_name)
        return cls()

    class Java:
        implements = ["com.alibaba.alink.common.pyrunner.PyMainHandle"]


gateway: Optional[JavaGateway] = None


def main():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    p = OptionParser()
    p.add_option('-j', '--jvm_port', help='the port for jvm side',
                 dest='jvm_port', type=int, default=None)
    p.add_option('-p', '--py_port', help='the port for python side',
                 dest='py_port', type=int, default=None)
    (z, args) = p.parse_args()

    app = PyMain()
    global gateway
    gateway = JavaGateway(
        gateway_parameters=GatewayParameters(port=z.jvm_port, auto_field=True, auto_convert=True),
        callback_server_parameters=CallbackServerParameters(
            port=z.py_port, daemonize=True,
            daemonize_connections=False,
            propagate_java_exceptions=True),
        python_server_entry_point=app)
    print('Started Listening On {}'.format(gateway.get_callback_server().get_listening_port()))
    sys.stdout.flush()
    while app.check():
        # Increase sleep time, since Java will kill Python process anyway
        time.sleep(60.0)
    print('Exit', flush=True)


if __name__ == '__main__':
    main()
