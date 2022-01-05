package com.alibaba.alink.operator.stream.source;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.pravega.plugin.PravegaClassLoaderFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.Table;

@IoOpAnnotation(name = "pravega", ioType = IOType.SourceStream)
public class PravegaSourceStreamOp extends BaseSourceStreamOp<PravegaSourceStreamOp> {

	private PravegaClassLoaderFactory factory;

	public PravegaSourceStreamOp() {
		this(new Params());
	}

	public PravegaSourceStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(PravegaSourceStreamOp.class), params);
	}

	@Override
	protected Table initializeDataSource() {
		if (factory == null) {
			factory = new PravegaClassLoaderFactory("0001");
		}

		Tuple1<RichParallelSourceFunction> sourceFunction = PravegaClassLoaderFactory
			.create(factory)
			.createPravegaSourceFunction(getParams());

		return DataStreamConversionUtil.toTable(
			getMLEnvironmentId(),
			MLEnvironmentFactory.get(getMLEnvironmentId())
				.getStreamExecutionEnvironment()
				.addSource(sourceFunction.f0),
			new String[]{}
		);
	}
}
