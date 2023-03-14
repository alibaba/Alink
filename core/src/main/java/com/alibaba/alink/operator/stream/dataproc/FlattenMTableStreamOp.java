package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.FlattenMTableMapper;
import com.alibaba.alink.operator.stream.utils.FlatMapStreamOp;
import com.alibaba.alink.params.dataproc.FlattenMTableParams;

/**
 * Transform MTable format recommendation to table format.
 */
@ParamSelectColumnSpec(name = "selectedCol",
	allowedTypeCollections = TypeCollections.MTABLE_TYPES)
@NameCn("MTable展开")
@NameEn("Flatten MTable")
public class FlattenMTableStreamOp extends FlatMapStreamOp <FlattenMTableStreamOp>
	implements FlattenMTableParams <FlattenMTableStreamOp> {

	private static final long serialVersionUID = 790348573681664909L;

	public FlattenMTableStreamOp() {
		this(null);
	}

	public FlattenMTableStreamOp(Params params) {
		super(FlattenMTableMapper::new, params);
	}
}
