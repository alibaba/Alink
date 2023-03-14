package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.ToMTableMapper;
import com.alibaba.alink.params.dataproc.ToMTableParams;

/**
 * batch op for transforming to MTable.
 */
@NameCn("转MTable")
@NameEn("To MTable")
public class ToMTableBatchOp extends MapBatchOp <ToMTableBatchOp>
	implements ToMTableParams <ToMTableBatchOp> {

	public ToMTableBatchOp() {
		this(new Params());
	}

	public ToMTableBatchOp(Params params) {
		super(ToMTableMapper::new, params);
	}

}
