package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.kafka.plugin.KafkaClassLoaderFactory;
import com.alibaba.alink.common.io.kafka.plugin.RichSinkFunctionWithClassLoader;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.KafkaSinkParams;

@IoOpAnnotation(name = "kafka", ioType = IOType.SinkStream)
public class KafkaSinkStreamOp extends BaseSinkStreamOp <KafkaSinkStreamOp>
	implements KafkaSinkParams <KafkaSinkStreamOp> {

	private KafkaClassLoaderFactory factory;

	public KafkaSinkStreamOp() {
		this(new Params());
	}

	public KafkaSinkStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(KafkaSinkStreamOp.class), params);
	}

	@Override
	protected KafkaSinkStreamOp sinkFrom(StreamOperator <?> in) {
		if (factory == null) {
			factory = new KafkaClassLoaderFactory(getParams().get(KafkaSinkParams.PLUGIN_VERSION));
		}

		in.getDataStream()
			.addSink(new RichSinkFunctionWithClassLoader(
					factory,
					KafkaClassLoaderFactory.create(factory).createKafkaSinkFunction(getParams(), in.getSchema())
				)
			);

		return this;
	}
}
