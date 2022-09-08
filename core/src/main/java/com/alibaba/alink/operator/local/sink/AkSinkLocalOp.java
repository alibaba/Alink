package com.alibaba.alink.operator.local.sink;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.io.filesystem.AkUtils.AkMeta;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.partition.AkSinkCollectorCreator;
import com.alibaba.alink.operator.common.io.partition.LocalUtils;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.io.AkSinkBatchParams;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Sink local op data to a file system with ak format. Ak is a file format define by alink.
 */
@NameCn("AK文件导出")
public final class AkSinkLocalOp extends BaseSinkLocalOp <AkSinkLocalOp>
	implements AkSinkBatchParams <AkSinkLocalOp> {

	public AkSinkLocalOp() {
		this(new Params());
	}

	public AkSinkLocalOp(Params params) {
		super(params);
	}

	@Override
	public AkSinkLocalOp linkFrom(LocalOperator <?>... inputs) {
		return sinkFrom(checkAndGetFirst(inputs));
	}

	@Override
	public AkSinkLocalOp sinkFrom(LocalOperator <?> in) {
		if (getPartitionCols() != null) {

			LocalUtils.partitionAndWriteFile(
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
			output(
				in.getOutputTable().getRows(),
				new AkUtils.AkOutputFormat(
					getFilePath(),
					new AkMeta(TableUtil.schema2SchemaStr(in.getSchema())),
					getOverwriteSink() ? FileSystem.WriteMode.OVERWRITE : FileSystem.WriteMode.NO_OVERWRITE),
				getNumFiles()
			);
		}

		return this;
	}
}
