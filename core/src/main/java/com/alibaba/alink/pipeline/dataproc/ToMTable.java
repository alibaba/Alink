package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.ToMTableMapper;
import com.alibaba.alink.params.dataproc.ToMTableParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Transforming to MTable.
 */
public class ToMTable extends MapTransformer <ToMTable>
	implements ToMTableParams <ToMTable> {

	public ToMTable() {
		this(new Params());
	}

	public ToMTable(Params params) {
		super(ToMTableMapper::new, params);
	}

}
