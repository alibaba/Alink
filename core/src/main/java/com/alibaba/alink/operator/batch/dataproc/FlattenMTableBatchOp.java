package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.FlatMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.FlattenMTableMapper;
import com.alibaba.alink.params.dataproc.FlattenMTableParams;

/**
 * Transform MTable format recommendation to table format.
 */
@ParamSelectColumnSpec(name = "selectedCol",
	allowedTypeCollections = TypeCollections.MTABLE_TYPES)
@ParamSelectColumnSpec(name = "reservedCols")
@NameCn("MTable展开")
@NameEn("Flatten MTable")
public class FlattenMTableBatchOp extends FlatMapBatchOp <FlattenMTableBatchOp>
	implements FlattenMTableParams <FlattenMTableBatchOp> {

	private static final long serialVersionUID = 790348573681664909L;

	public FlattenMTableBatchOp() {
		this(null);
	}

	public FlattenMTableBatchOp(Params params) {
		super(FlattenMTableMapper::new, params);
	}
}
