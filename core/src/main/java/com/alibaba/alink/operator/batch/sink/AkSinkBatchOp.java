package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.AkUtils.AkMeta;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.utils.DataSetUtil;
import com.alibaba.alink.operator.common.io.partition.AkSinkCollectorCreator;
import com.alibaba.alink.params.io.AkSinkBatchParams;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Sink batch op data to a file system with ak format. Ak is a file format define by alink.
 */
@IoOpAnnotation(name = "ak", ioType = IOType.SinkBatch)
@NameCn("AK文件导出")
@NameEn("Ak Sink")
public final class AkSinkBatchOp extends BaseSinkBatchOp <AkSinkBatchOp>
	implements AkSinkBatchParams <AkSinkBatchOp> {

	private static final long serialVersionUID = -6701780409272076102L;

	public AkSinkBatchOp() {
		this(new Params());
	}

	public AkSinkBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(AkSourceBatchOp.class), params);
	}

	@Override
	public AkSinkBatchOp linkFrom(BatchOperator <?>... inputs) {
		return sinkFrom(checkAndGetFirst(inputs));
	}

	@Override
	public AkSinkBatchOp sinkFrom(BatchOperator <?> in) {
		if (getPartitionCols() != null) {

			DataSetUtil.partitionAndWriteFile(
				in,
				new AkSinkCollectorCreator(
					new AkMeta(
						TableUtil.schema2SchemaStr(
							new TableSchema(
								ArrayUtils.removeElements(in.getColNames(), getPartitionCols()),
								TableUtil.findColTypes(in.getSchema(),
									ArrayUtils.removeElements(in.getColNames(), getPartitionCols()))
							)
						)
					)
				),
				getParams()
			);

		} else {
			in.getDataSet()
				.output(
					new AkUtils.AkOutputFormat(
						getFilePath(),
						new AkUtils.AkMeta(TableUtil.schema2SchemaStr(in.getSchema())),
						getOverwriteSink() ? FileSystem.WriteMode.OVERWRITE : FileSystem.WriteMode.NO_OVERWRITE)
				)
				.name("AkSink")
				.setParallelism(getNumFiles());
		}

		return this;
	}

}
