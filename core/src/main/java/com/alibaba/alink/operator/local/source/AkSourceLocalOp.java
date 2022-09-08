package com.alibaba.alink.operator.local.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.operator.common.io.partition.AkSourceCollectorCreator;
import com.alibaba.alink.operator.common.io.partition.LocalUtils;
import com.alibaba.alink.params.io.AkSourceParams;

import java.io.IOException;
import java.util.List;

/**
 * Read an ak file from file system.
 */
@NameCn("AK文件读入")
public final class AkSourceLocalOp extends BaseSourceLocalOp <AkSourceLocalOp>
	implements AkSourceParams <AkSourceLocalOp> {

	public AkSourceLocalOp() {
		this(new Params());
	}

	public AkSourceLocalOp(Params params) {
		super(params);
	}

	@Override
	public MTable initializeDataSource() {
		final AkUtils.AkMeta meta;
		try {
			meta = AkUtils.getMetaFromPath(getFilePath());
		} catch (IOException e) {
			throw new AkIllegalOperatorParameterException(
				"Could not get meta from ak file: " + getFilePath().getPathStr(), e
			);
		}

		String partitions = getPartitions();

		if (partitions == null) {
			return new MTable(
				createInput(new AkUtils.AkInputFormat(getFilePath(), meta), getParams()),
				meta.schemaStr
			);
		} else {
			try {
				Tuple2 <List <Row>, TableSchema> schemaAndData =
					LocalUtils.readFromPartitionLocal(getParams(), new AkSourceCollectorCreator(meta));

				return new MTable(schemaAndData.f0, schemaAndData.f1);
			} catch (IOException e) {
				throw new AkIllegalOperatorParameterException("Error. ", e);
			}
		}
	}

}
