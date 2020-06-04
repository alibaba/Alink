//
// Copyright (c) 2014, Alibaba Inc.
// All rights reserved.
//
// Author: Yan Huang <allison.hy@alibaba-inc.com>
// Created: 6/12/18
// Description:
//

package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.TripleToVectorParams;
import org.apache.flink.ml.api.misc.param.Params;

public final class TripleToVectorBatchOp extends TripleToAnyBatchOp<TripleToVectorBatchOp>
	implements TripleToVectorParams<TripleToVectorBatchOp> {

	public TripleToVectorBatchOp() {
		this(new Params());
	}

	public TripleToVectorBatchOp(Params params) {
		super(FormatType.VECTOR, params.clone());
	}
}
