from IPython import get_ipython
from IPython.core.magic import (register_line_magic)
from IPython.core.magic_arguments import magic_arguments, argument, parse_argstring

if get_ipython() is not None:
    @register_line_magic
    @magic_arguments()
    @argument('parallelism', type=int, help='parallelism')
    @argument('--flinkHome', type=str, default=None, help='flink home')
    @argument('--config', type=object, default=None, help='additional config for running tasks')
    def use_local_env(line):
        args = parse_argstring(use_local_env, line)
        from pyalink.alink.env import useLocalEnv
        useLocalEnv(**vars(args))


    @register_line_magic
    @magic_arguments()
    @argument('host', type=str, help='host of Flink cluster')
    @argument('port', type=int, help='port of Flink cluster')
    @argument('parallelism', type=int, help='parallelism')
    @argument('--flinkHome', type=str, default=None, help='flink home')
    @argument('--localIp', type=str, default="localhost",
              help='external ip address, which is used for DataStream display')
    @argument('--shipAlinkAlgoJar', type=bool, default=True, help='whether to ship Alink algorithm jar to the cluster')
    @argument('--config', type=object, default=None, help='additional config for running tasks')
    def use_remote_env(line):
        args = parse_argstring(use_remote_env, line)
        from pyalink.alink.env import useRemoteEnv
        useRemoteEnv(**vars(args))
