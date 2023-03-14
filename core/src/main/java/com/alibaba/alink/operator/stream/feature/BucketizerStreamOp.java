package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.feature.BucketizerMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.feature.BucketizerParams;

/**
 * Map a continuous variable into several buckets.
 * It supports a single column input or multiple columns input. If input is a single column, selectedColName,
 * outputColName and splits should be set. If input are multiple columns, selectedColNames, outputColnames
 * and splitsArray should be set, and the lengths of them should be equal. In the case of multiple columns,
 * each column used the corresponding splits.
 */
@NameCn("分桶")
@NameEn("Bucketizer")
public class BucketizerStreamOp extends MapStreamOp <BucketizerStreamOp>
	implements BucketizerParams <BucketizerStreamOp> {
	private static final long serialVersionUID = -3837154543787425055L;

	public BucketizerStreamOp() {
		this(null);
	}

	public BucketizerStreamOp(Params params) {
		super(BucketizerMapper::new, params);
	}
}
