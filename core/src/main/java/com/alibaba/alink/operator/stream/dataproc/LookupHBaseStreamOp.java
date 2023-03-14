package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.LookupHBaseMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.LookupHBaseParams;

/**
 * stream op for lookup from hbase.
 */
@ParamSelectColumnSpec(name = "rowKeyCol", allowedTypeCollections = TypeCollections.STRING_TYPE)
@NameCn("添加HBase数据")
@NameEn("Lookup HBase Operation")
public class LookupHBaseStreamOp extends MapStreamOp <LookupHBaseStreamOp>
	implements LookupHBaseParams <LookupHBaseStreamOp> {

	public LookupHBaseStreamOp() {
		this(new Params());
	}

	public LookupHBaseStreamOp(Params params) {
		super(LookupHBaseMapper::new, params);
	}

}
