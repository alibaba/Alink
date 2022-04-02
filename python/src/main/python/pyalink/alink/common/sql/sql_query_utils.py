import re

__all__ = ['register_table_name', 'sql_query']

batch_table_name_map = dict()
stream_table_name_map = dict()


def register_table_name(op, name: str, op_type: str):
    if op_type == "batch":
        batch_table_name_map[name] = op
    elif op_type == "stream":
        stream_table_name_map[name] = op
    else:
        raise Exception("op_type should be 'batch' or 'stream'.")


def clear_table_names():
    batch_table_name_map.clear()
    stream_table_name_map.clear()


def sql_query(query: str, op_type: str):
    if op_type == "batch":
        from pyalink.alink.batch.common import PySqlCmdBatchOp
        table_name_map = batch_table_name_map
        sql_cmd_op_cls = PySqlCmdBatchOp
    elif op_type == "stream":
        table_name_map = stream_table_name_map
        from pyalink.alink.stream.common import PySqlCmdStreamOp
        sql_cmd_op_cls = PySqlCmdStreamOp
    else:
        raise Exception("op_type should be 'batch' or 'stream'.")

    counter = 0
    ops = []
    for (name, op) in table_name_map.items():
        pattern = "\\b" + name + "\\b"
        match = re.findall(pattern, query)
        if match is None or len(match) == 0:
            continue
        ops.append(op)
        counter += 1
    sql_cmd_op = sql_cmd_op_cls().setCommand(query)
    sql_cmd_op.linkFrom(*ops)
    return sql_cmd_op
