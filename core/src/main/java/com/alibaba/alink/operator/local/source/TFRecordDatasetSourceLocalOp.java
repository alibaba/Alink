package com.alibaba.alink.operator.local.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.filesystem.TFRecordDatasetUtils.TFRecordDatasetInputFormat;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.io.TFRecordDatasetSourceParams;

/**
 * Read a TFRecordDataset file from file system.
 */
@NameCn("TFRecordDataset文件读入")
public final class TFRecordDatasetSourceLocalOp extends BaseSourceLocalOp <TFRecordDatasetSourceLocalOp>
	implements TFRecordDatasetSourceParams <TFRecordDatasetSourceLocalOp> {

	public TFRecordDatasetSourceLocalOp() {
		this(new Params());
	}

	public TFRecordDatasetSourceLocalOp(Params params) {
		super(params);
	}

	@Override
	public MTable initializeDataSource() {
		String schemaStr = getSchemaStr();
		TableSchema schema = TableUtil.schemaStr2Schema(schemaStr);
		return new MTable(
			createInput(new TFRecordDatasetInputFormat(getFilePath(), schemaStr), getParams()),
			schema);
	}
}
