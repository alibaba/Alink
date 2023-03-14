package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.kafka.plugin.KafkaClassLoaderFactory;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.KafkaSinkParams;

@IoOpAnnotation(name = "kafka", ioType = IOType.SinkStream)
@NameCn("Kafka导出")
@NameEn("Kafka Sink")
public class KafkaSinkStreamOp extends BaseSinkStreamOp <KafkaSinkStreamOp>
	implements KafkaSinkParams <KafkaSinkStreamOp> {

	private final KafkaClassLoaderFactory factory;

	public KafkaSinkStreamOp() {
		this(new Params());
	}

	public KafkaSinkStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(KafkaSinkStreamOp.class), params);

		factory = new KafkaClassLoaderFactory("0.11");
	}

	@Override
	protected KafkaSinkStreamOp sinkFrom(StreamOperator <?> in) {

		in.getDataStream()
			.addSink(new RichSinkFunctionWithClassLoader(
					factory,
					KafkaClassLoaderFactory.create(factory).createKafkaSinkFunction(getParams(), in.getSchema())
				)
			);

		return this;
	}
}
