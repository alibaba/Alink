package com.alibaba.alink.operator.stream.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.io.AkSourceParams;

@IoOpAnnotation(name = "ak", ioType = IOType.SourceStream)
public final class AkSourceStreamOp extends BaseSourceStreamOp<AkSourceStreamOp>
	implements AkSourceParams<AkSourceStreamOp> {

	public AkSourceStreamOp() {
		this(new Params());
	}

	public AkSourceStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(AkSourceBatchOp.class), params);
	}

	@Override
	public Table initializeDataSource() {
		final AkUtils.AkMeta meta = AkUtils.getMetaFromPath(getFilePath());

		return DataStreamConversionUtil.toTable(
			getMLEnvironmentId(),
			MLEnvironmentFactory
				.get(getMLEnvironmentId())
				.getStreamExecutionEnvironment()
				.createInput(new AkUtils.AkInputFormat(getFilePath(), meta))
				.name("AkSource")
				.rebalance(),
			CsvUtil.schemaStr2Schema(meta.schemaStr)
		);
	}
}
