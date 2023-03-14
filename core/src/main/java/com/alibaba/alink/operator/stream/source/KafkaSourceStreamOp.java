package com.alibaba.alink.operator.stream.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.kafka.plugin.KafkaClassLoaderFactory;
import com.alibaba.alink.operator.stream.utils.DataStreamConversionUtil;
import com.alibaba.alink.params.io.KafkaSourceParams;

@IoOpAnnotation(name = "kafka", ioType = IOType.SourceStream)
@NameCn("流式Kafka输入")
@NameEn("Kafka Source")
public class KafkaSourceStreamOp extends BaseSourceStreamOp <KafkaSourceStreamOp>
	implements KafkaSourceParams <KafkaSourceStreamOp> {

	private final KafkaClassLoaderFactory factory;

	public KafkaSourceStreamOp() {
		this(new Params());
	}

	public KafkaSourceStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(KafkaSourceStreamOp.class), params);

		factory = new KafkaClassLoaderFactory("0.11");
	}

	@Override
	protected Table initializeDataSource() {
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
