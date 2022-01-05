package com.alibaba.alink.operator.stream.sink;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.pravega.plugin.PravegaClassLoaderFactory;
import com.alibaba.alink.operator.stream.StreamOperator;
import org.apache.flink.ml.api.misc.param.Params;

@IoOpAnnotation(name = "pravega", ioType = IOType.SinkStream)
public class PravegaSinkStreamOp extends BaseSinkStreamOp<PravegaSinkStreamOp> {

	private PravegaClassLoaderFactory factory;

	public PravegaSinkStreamOp() {
		this(new Params());
	}

	public PravegaSinkStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(PravegaSinkStreamOp.class), params);
	}

	@Override
	protected PravegaSinkStreamOp sinkFrom(StreamOperator<?> in) {
		if (factory == null) {
			factory = new PravegaClassLoaderFactory("0001");
		}

		in.getDataStream().addSink(PravegaClassLoaderFactory.create(factory).createPravegaSinkFunction(getParams()).f0);
		return this;
	}
}
