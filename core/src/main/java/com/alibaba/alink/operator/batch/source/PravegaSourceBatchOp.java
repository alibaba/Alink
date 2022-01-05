package com.alibaba.alink.operator.batch.source;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.pravega.plugin.PravegaClassLoaderFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.stream.source.BaseSourceStreamOp;
import com.alibaba.alink.params.io.KafkaSourceParams;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.Table;

@IoOpAnnotation(name = "pravega", ioType = IOType.SourceStream)
public class PravegaSourceBatchOp extends BaseSourceStreamOp<PravegaSourceBatchOp>
	implements KafkaSourceParams<PravegaSourceBatchOp> {

	private PravegaClassLoaderFactory factory;

	public PravegaSourceBatchOp() {
		this(new Params());
	}

	public PravegaSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(PravegaSourceBatchOp.class), params);
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
