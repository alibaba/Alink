package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.io.AkSinkParams;

@IoOpAnnotation(name = "ak", ioType = IOType.SinkBatch)
public final class AkSinkBatchOp extends BaseSinkBatchOp<AkSinkBatchOp>
	implements AkSinkParams<AkSinkBatchOp> {

	public AkSinkBatchOp() {
		this(new Params());
	}

	public AkSinkBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(AkSourceBatchOp.class), params);
	}

	@Override
	public AkSinkBatchOp sinkFrom(BatchOperator in) {
		in.getDataSet()
			.output(
				new AkUtils.AkOutputFormat(
					getFilePath(),
					new AkUtils.AkMeta(CsvUtil.schema2SchemaStr(in.getSchema())),
					getOverwriteSink() ? FileSystem.WriteMode.OVERWRITE : FileSystem.WriteMode.NO_OVERWRITE)
			)
			.name("AkSink")
			.setParallelism(getNumFiles());

		return this;
	}

}
