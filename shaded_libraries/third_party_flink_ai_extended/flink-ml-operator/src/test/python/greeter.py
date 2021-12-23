from __future__ import print_function
import sys


def map_func(context):
    print('hello from greeter')
    print('index:', context.index)
    sys.stdout.flush()
