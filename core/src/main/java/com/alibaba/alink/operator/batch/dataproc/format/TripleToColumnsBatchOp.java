package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.TripleToColumnsParams;

/**
 * Transform data type from Triple to Columns.
 */
@ParamSelectColumnSpec(name = "tripleColumnCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("三元组转列数据")
public class TripleToColumnsBatchOp extends TripleToAnyBatchOp <TripleToColumnsBatchOp>
	implements TripleToColumnsParams <TripleToColumnsBatchOp> {

	private static final long serialVersionUID = -6995665589140798713L;

	public TripleToColumnsBatchOp() {
		this(new Params());
	}

	public TripleToColumnsBatchOp(Params params) {
		super(FormatType.COLUMNS, params);
	}
}
