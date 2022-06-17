package com.alibaba.alink.operator.stream.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.AkUtils;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.common.io.partition.AkSourceCollectorCreator;
import com.alibaba.alink.operator.common.io.partition.Utils;
import com.alibaba.alink.params.io.AkSourceParams;

import java.io.IOException;

/**
 * Create a stream with a ak file from file system.
 */
@IoOpAnnotation(name = "ak", ioType = IOType.SourceStream)
@NameCn("AK文件数据源")
public final class AkSourceStreamOp extends BaseSourceStreamOp <AkSourceStreamOp>
	implements AkSourceParams <AkSourceStreamOp> {
	private static final long serialVersionUID = -1632712937397561402L;

	public AkSourceStreamOp() {
		this(new Params());
	}

	public AkSourceStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(AkSourceBatchOp.class), params);
	}

	@Override
	public Table initializeDataSource() {
		final AkUtils.AkMeta meta;
		try {
			meta = AkUtils.getMetaFromPath(getFilePath());
		} catch (IOException e) {
			throw new IllegalArgumentException(
				"Could not get meta from ak file: " + getFilePath().getPathStr(), e
			);
		}

		String partitions = getPartitions();

		if (partitions == null) {
			return DataStreamConversionUtil.toTable(
				getMLEnvironmentId(),
				MLEnvironmentFactory
					.get(getMLEnvironmentId())
					.getStreamExecutionEnvironment()
					.createInput(new AkUtils.AkInputFormat(getFilePath(), meta))
					.name("AkSource")
					.rebalance(),
				TableUtil.schemaStr2Schema(meta.schemaStr)
			);
		} else {

			try {

				Tuple2 <DataStream <Row>, TableSchema> schemaAndData =
					Utils.readFromPartitionStream(
						getParams(), getMLEnvironmentId(),
						new AkSourceCollectorCreator(meta)
					);

				return DataStreamConversionUtil.toTable(getMLEnvironmentId(), schemaAndData.f0, schemaAndData.f1);

			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}
}
