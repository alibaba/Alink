package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.feature.BucketizerMapper;
import com.alibaba.alink.params.feature.BucketizerParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Map a continuous variable into several buckets.
 *
 * It supports a single column input or multiple columns input. If input is a single column, selectedColName,
 * outputColName and splits should be set. If input are multiple columns, selectedColNames, outputColnames
 * and splitsArray should be set, and the lengths of them should be equal. In the case of multiple columns,
 * each column used the corresponding splits.
 *
 * Split array must be strictly increasing and have at least three points. It's a string input with split points
 * segments with delimiter ",".
 */
public class Bucketizer extends MapTransformer<Bucketizer>
	implements BucketizerParams <Bucketizer> {

	public Bucketizer() {
		this(null);
	}

	public Bucketizer(Params params) {
		super(BucketizerMapper::new, params);
	}
}
