import pyflink
from pyflink.dataset import ExecutionEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import BatchTableEnvironment
from pyflink.table import StreamTableEnvironment

from pyalink.alink import *

benv = ExecutionEnvironment.get_execution_environment()
senv = StreamExecutionEnvironment.get_execution_environment()

btenv = BatchTableEnvironment.create(benv)
stenv = StreamTableEnvironment.create(senv)

mlenv = useCustomEnv(pyflink.java_gateway.get_gateway(),
                     benv, btenv, senv, stenv)

t = stenv.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
source = TableSourceStreamOp(t)
source.print()
StreamOperator.execute()
