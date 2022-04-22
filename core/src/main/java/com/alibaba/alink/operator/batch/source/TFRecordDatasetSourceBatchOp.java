package com.alibaba.alink.operator.batch.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.TFRecordDatasetUtils.TFRecordDatasetInputFormat;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.io.TFRecordDatasetSourceParams;

/**
 * Read a TFRecordDataset file from file system.
 */
@IoOpAnnotation(name = "tfrecord", ioType = IOType.SourceBatch)
@NameCn("TFRecordDataset文件读入")
public final class TFRecordDatasetSourceBatchOp extends BaseSourceBatchOp <TFRecordDatasetSourceBatchOp>
	implements TFRecordDatasetSourceParams <TFRecordDatasetSourceBatchOp> {

	private static final long serialVersionUID = 7493386303148970332L;

	public TFRecordDatasetSourceBatchOp() {
		this(new Params());
	}

	public TFRecordDatasetSourceBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(TFRecordDatasetSourceBatchOp.class), params);
	}

	@Override
	public Table initializeDataSource() {
		String schemaStr = getSchemaStr();
		TableSchema schema = TableUtil.schemaStr2Schema(schemaStr);
		return DataSetConversionUtil.toTable(
			getMLEnvironmentId(),
			MLEnvironmentFactory
				.get(getMLEnvironmentId())
				.getExecutionEnvironment()
				.createInput(new TFRecordDatasetInputFormat(getFilePath(), schemaStr))
				.name("TFRecordDatasetSource")
				.rebalance(),
			schema);
	}
}
