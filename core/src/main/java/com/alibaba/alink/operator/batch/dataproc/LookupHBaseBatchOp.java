package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.LookupHBaseMapper;
import com.alibaba.alink.params.dataproc.LookupHBaseParams;

/**
 * batch op for lookup from hbase.
 */
@ParamSelectColumnSpec(name = "rowKeyCol", allowedTypeCollections = TypeCollections.STRING_TYPE)
@NameCn("添加HBase数据")
public class LookupHBaseBatchOp extends MapBatchOp <LookupHBaseBatchOp>
	implements LookupHBaseParams <LookupHBaseBatchOp> {

	public LookupHBaseBatchOp() {
		this(new Params());
	}

	public LookupHBaseBatchOp(Params params) {
		super(LookupHBaseMapper::new, params);
	}

}
