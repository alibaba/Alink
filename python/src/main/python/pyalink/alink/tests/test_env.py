import unittest

import pytest

from pyalink.alink import *


class TestEnvironment(unittest.TestCase):

    @pytest.mark.usefixtures()
    def test_switch_env(self):
        useLocalEnv(2)
        resetEnv()
        useRemoteEnv("localhost", 8081, 2)
        resetEnv()

    @pytest.mark.usefixtures()
    def test_only_reset_env(self):
        resetEnv()

    @pytest.mark.usefixtures()
    def test_only_local_env(self):
        mlenv = useLocalEnv(2)
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        source.firstN(5).print()

    @pytest.mark.usefixtures()
    def test_only_get_mlenv(self):
        mlenv = getMLEnv()
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        source.firstN(5).print()

        env, btenv, senv, stenv = mlenv
        t = stenv.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        source = TableSourceStreamOp(t)
        source.print()
        StreamOperator.execute()

    @pytest.mark.usefixtures()
    def test_mlenv_in_local_env(self):
        mlenv = useLocalEnv(2)
        t = mlenv.btenv.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        source = TableSourceBatchOp(t)
        source.print()

    @pytest.mark.usefixtures()
    def test_get_mlenv_in_local_env(self):
        useLocalEnv(2)
        mlenv = getMLEnv()
        t = mlenv.btenv.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        source = TableSourceBatchOp(t)
        source.print()

    @pytest.mark.usefixtures()
    def test_custom_env(self):
        import pyflink
        from pyflink.dataset import ExecutionEnvironment
        from pyflink.datastream import StreamExecutionEnvironment
        benv = ExecutionEnvironment.get_execution_environment()
        senv = StreamExecutionEnvironment.get_execution_environment()

        from pyflink.table import BatchTableEnvironment
        from pyflink.table import StreamTableEnvironment

        btenv = BatchTableEnvironment.create(benv)
        stenv = StreamTableEnvironment.create(senv)

        mlenv = useCustomEnv(pyflink.java_gateway.get_gateway(),
                             benv, btenv, senv, stenv)

        t = mlenv.btenv.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        source = TableSourceBatchOp(t)
        source.print()

        t = mlenv.stenv.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        source = TableSourceStreamOp(t)
        source.print()
        StreamOperator.execute()

        from pyalink.alink import env
        env._in_custom_env = False
        resetEnv()
