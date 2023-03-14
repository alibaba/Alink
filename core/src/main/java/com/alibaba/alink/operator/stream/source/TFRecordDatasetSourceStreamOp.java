package com.alibaba.alink.operator.stream.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.TFRecordDatasetUtils.TFRecordDatasetInputFormat;
import com.alibaba.alink.operator.stream.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.params.io.TFRecordDatasetSourceParams;

/**
 * Read a TFRecordDataset file from file system.
 */
@IoOpAnnotation(name = "tfrecord", ioType = IOType.SourceStream)
@NameCn("TFRecordDataset文件读入")
@NameEn("TFRecord Dataset Source")
public final class TFRecordDatasetSourceStreamOp extends BaseSourceStreamOp <TFRecordDatasetSourceStreamOp>
	implements TFRecordDatasetSourceParams <TFRecordDatasetSourceStreamOp> {
	
	private static final long serialVersionUID = -4815399313309024935L;

	public TFRecordDatasetSourceStreamOp() {
		this(new Params());
	}

	public TFRecordDatasetSourceStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(AkSourceBatchOp.class), params);
	}

	@Override
	public Table initializeDataSource() {
		String schemaStr = getSchemaStr();
		TableSchema schema = TableUtil.schemaStr2Schema(schemaStr);
		return DataStreamConversionUtil.toTable(
			getMLEnvironmentId(),
			MLEnvironmentFactory
				.get(getMLEnvironmentId())
				.getStreamExecutionEnvironment()
				.createInput(new TFRecordDatasetInputFormat(getFilePath(), schemaStr))
				.name("TFRecordDatasetSource")
				.rebalance(),
			schema);
	}
}
