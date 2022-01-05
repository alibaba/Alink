package com.alibaba.alink.common.io.pravega.plugin;

import com.alibaba.alink.operator.common.io.serde.RowToCsvSerialization;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.connectors.flink.FlinkPravegaInputFormat;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import java.net.URI;

public class PravegaSourceSinkInPluginFactory implements PravegaSourceSinkFactory {


	@Override
	public Tuple1<RichParallelSourceFunction> createPravegaSourceFunction(Params params) {
		String DEFAULT_SCOPE = params.getStringOrDefault("scope", "scope");
		String Default_URI_PARAM = params.getStringOrDefault("controller", "controller");
		String STREAM_PARAM = params.getStringOrDefault("stream", "stream");

		PravegaConfig pravegaConfig = PravegaConfig
				.fromParams(null)
				.withControllerURI(URI.create(Default_URI_PARAM))
				.withDefaultScope(DEFAULT_SCOPE);
		Stream stream = pravegaConfig.resolve(STREAM_PARAM);
		try(StreamManager streamManager = StreamManager.create(pravegaConfig.getClientConfig())) {
			streamManager.createScope(stream.getScope());
			streamManager.createStream(stream.getScope(), stream.getStreamName(), StreamConfiguration.builder().build());
		}
		FlinkPravegaReader<String> source = FlinkPravegaReader.<String>builder()
				.withPravegaConfig(pravegaConfig)
				.forStream(stream)
				.withDeserializationSchema(new SimpleStringSchema())
				.build();
		return Tuple1.of(source);
	}


	@Override
	public Tuple1<FlinkPravegaInputFormat> createPravegaSourceFunctionBatch(Params params) {
		String DEFAULT_SCOPE = params.getStringOrDefault("scope", "scope");
		String Default_URI_PARAM = params.getStringOrDefault("controller", "controller");
		String STREAM_PARAM = params.getStringOrDefault("stream", "stream");
		PravegaConfig pravegaConfig = PravegaConfig
				.fromParams(null)
				.withControllerURI(URI.create(Default_URI_PARAM))
				.withDefaultScope(DEFAULT_SCOPE);
		Stream stream = pravegaConfig.resolve(STREAM_PARAM);
		try(StreamManager streamManager = StreamManager.create(pravegaConfig.getClientConfig())) {
			streamManager.createScope(stream.getScope());
			streamManager.createStream(stream.getScope(), stream.getStreamName(), StreamConfiguration.builder().build());
		}
		FlinkPravegaInputFormat<String> source = FlinkPravegaInputFormat.<String>builder()
				.forStream(stream)
				.withPravegaConfig(pravegaConfig)
				.withDeserializationSchema(new SimpleStringSchema())
				.build();
		return Tuple1.of(source);
	}

	@Override
	public Tuple1<RichSinkFunction> createPravegaSinkFunction(Params params) {
		String DEFAULT_SCOPE = params.getStringOrDefault("scope", "scope");
		String Default_URI_PARAM = params.getStringOrDefault("controller", "controller");
		String STREAM_PARAM = params.getStringOrDefault("stream", "stream");
		String fieldDelim = params.getStringOrDefault("fieldDelim", ",");
		PravegaConfig pravegaConfig = PravegaConfig
				.fromParams(null)
				.withControllerURI(URI.create(Default_URI_PARAM))
				.withDefaultScope(DEFAULT_SCOPE);
		Stream stream = pravegaConfig.resolve(STREAM_PARAM);
		try(StreamManager streamManager = StreamManager.create(pravegaConfig.getClientConfig())) {
			streamManager.createScope(stream.getScope());
			streamManager.createStream(stream.getScope(), stream.getStreamName(), StreamConfiguration.builder().build());
			FlinkPravegaWriter<Row> writer = FlinkPravegaWriter.<Row>builder()
					.withPravegaConfig(pravegaConfig)
					.forStream(stream)
					.withSerializationSchema(new RowToCsvSerialization(new TypeInformation[]{TypeInformation.of(String.class)}, fieldDelim))
					.build();
			return Tuple1.of(writer);
		}

	}
}
