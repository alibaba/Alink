package com.alibaba.alink.operator.batch.source;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.partition.AkSourceCollectorCreator;
import com.alibaba.alink.operator.common.io.partition.Utils;
import com.alibaba.alink.params.io.AkSourceParams;

import java.io.IOException;

/**
 * Read an ak file from file system.
 */
@IoOpAnnotation(name = "ak", ioType = IOType.SourceBatch)
@NameCn("AK文件读入")
public final class AkSourceBatchOp extends BaseSourceBatchOp <AkSourceBatchOp>
	implements AkSourceParams <AkSourceBatchOp> {

	private static final long serialVersionUID = 7493386303148970332L;

	public AkSourceBatchOp() {
		this(new Params());
	}

	public AkSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(AkSourceBatchOp.class), params);
	}

	@Override
	public Table initializeDataSource() {
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
			return DataSetConversionUtil.toTable(
				getMLEnvironmentId(),
				MLEnvironmentFactory
					.get(getMLEnvironmentId())
					.getExecutionEnvironment()
					.createInput(new AkUtils.AkInputFormat(getFilePath(), meta))
					.name("AkSource")
					.rebalance(),
				TableUtil.schemaStr2Schema(meta.schemaStr)
			);
		} else {
			try {
				Tuple2 <DataSet <Row>, TableSchema> schemaAndData =
					Utils.readFromPartitionBatch(
						getParams(), getMLEnvironmentId(),
						new AkSourceCollectorCreator(meta)
					);

				return DataSetConversionUtil.toTable(getMLEnvironmentId(), schemaAndData.f0, schemaAndData.f1);
			} catch (IOException e) {
				throw new AkIllegalOperatorParameterException("Error. ",e);
			}
		}
	}

}
