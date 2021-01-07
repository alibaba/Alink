package com.alibaba.alink.operator.stream.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.kafka.plugin.KafkaClassLoaderFactory;
import com.alibaba.alink.common.io.kafka.plugin.RichParallelSourceFunctionWithClassLoader;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.params.io.KafkaSourceParams;

@IoOpAnnotation(name = "kafka", ioType = IOType.SourceStream)
public class KafkaSourceStreamOp extends BaseSourceStreamOp <KafkaSourceStreamOp>
	implements KafkaSourceParams <KafkaSourceStreamOp> {

	private KafkaClassLoaderFactory factory;

	public KafkaSourceStreamOp() {
		this(new Params());
	}

	public KafkaSourceStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(KafkaSourceStreamOp.class), params);
	}

	@Override
	protected Table initializeDataSource() {
		if (factory == null) {
			factory = new KafkaClassLoaderFactory(getParams().get(KafkaSourceParams.PLUGIN_VERSION));
		}

		Tuple2 <RichParallelSourceFunction <Row>, TableSchema> sourceFunction = KafkaClassLoaderFactory
			.create(factory)
			.createKafkaSourceFunction(getParams());

		return DataStreamConversionUtil.toTable(
			getMLEnvironmentId(),
			MLEnvironmentFactory.get(getMLEnvironmentId())
				.getStreamExecutionEnvironment()
				.addSource(new RichParallelSourceFunctionWithClassLoader(factory, sourceFunction.f0)),
			sourceFunction.f1
		);
	}
}
