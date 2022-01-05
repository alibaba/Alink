package com.alibaba.alink.common.io.pravega.plugin;

import io.pravega.connectors.flink.FlinkPravegaInputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public interface PravegaSourceSinkFactory {
	Tuple1<RichParallelSourceFunction> createPravegaSourceFunction(Params params);
	Tuple1<FlinkPravegaInputFormat> createPravegaSourceFunctionBatch(Params params);
	Tuple1<RichSinkFunction> createPravegaSinkFunction(Params params);
}
