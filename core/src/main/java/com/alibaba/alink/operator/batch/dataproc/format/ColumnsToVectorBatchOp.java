package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.ColumnsToVectorParams;

/**
 * Transform data type from Columns to Vector.
 */
@NameCn("列数据转向量")
@NameEn("table to vector")
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
public class ColumnsToVectorBatchOp extends BaseFormatTransBatchOp <ColumnsToVectorBatchOp>
	implements ColumnsToVectorParams <ColumnsToVectorBatchOp> {

	private static final long serialVersionUID = -2294570294275668326L;

	public ColumnsToVectorBatchOp() {
		this(new Params());
	}

	public ColumnsToVectorBatchOp(Params params) {
		super(FormatType.COLUMNS, FormatType.VECTOR, params);
	}
}
