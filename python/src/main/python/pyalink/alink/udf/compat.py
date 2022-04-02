"""
PyAlink reuses udf-related definitions in PyFlink.
However, PyFlink started support ScalarFunction in version 1.10, and started to support TableFunction in version 1.11.
Some definitions were changed from version 1.10 to 1.11.
Therefore, a compatability layer is added.
"""

try:
    # flink version >= 1.10
    # noinspection PyProtectedMember
    from pyflink.table.udf import UserDefinedFunction
except ImportError:
    from .user_defined_function import UserDefinedFunction

try:
    # flink version >= 1.10
    from pyflink.table.udf import udf, ScalarFunction
except ImportError:
    from .udf import udf, ScalarFunction

try:
    # flink version >= 1.11
    # noinspection PyProtectedMember
    from pyflink.table.udf import UserDefinedScalarFunctionWrapper
except ImportError:
    try:
        # flink version == 1.10
        # noinspection PyProtectedMember
        from pyflink.table.udf import UserDefinedFunctionWrapper as UserDefinedScalarFunctionWrapper
    except ImportError:
        from .udf import UserDefinedScalarFunctionWrapper

try:
    # flink version >= 1.11
    from pyflink.table.udf import udtf, TableFunction
except ImportError:
    from .udtf import udtf, TableFunction

try:
    # flink version >= 1.11
    # noinspection PyProtectedMember
    from pyflink.table.udf import UserDefinedTableFunctionWrapper
except ImportError:
    from .udtf import UserDefinedTableFunctionWrapper

__all__ = ['UserDefinedFunction',
           'udf', 'ScalarFunction', 'UserDefinedScalarFunctionWrapper',
           'udtf', 'TableFunction', 'UserDefinedTableFunctionWrapper']
