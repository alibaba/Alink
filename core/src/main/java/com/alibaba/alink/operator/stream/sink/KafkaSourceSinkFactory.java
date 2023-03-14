package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

public interface KafkaSourceSinkFactory {
	Tuple2 <RichParallelSourceFunction <Row>, TableSchema> createKafkaSourceFunction(Params params);

	RichSinkFunction <Row> createKafkaSinkFunction(Params params, TableSchema schema);
}
