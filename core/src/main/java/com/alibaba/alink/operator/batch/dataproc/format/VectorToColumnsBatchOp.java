package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.VectorToColumnsParams;

/**
 * Transform data type from Vector to Columns.
 */
@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("向量转列数据")
public class VectorToColumnsBatchOp extends BaseFormatTransBatchOp <VectorToColumnsBatchOp>
	implements VectorToColumnsParams <VectorToColumnsBatchOp> {

	private static final long serialVersionUID = 156209692588746206L;

	public VectorToColumnsBatchOp() {
		this(new Params());
	}

	public VectorToColumnsBatchOp(Params params) {
		super(FormatType.VECTOR, FormatType.COLUMNS, params);
	}
}
