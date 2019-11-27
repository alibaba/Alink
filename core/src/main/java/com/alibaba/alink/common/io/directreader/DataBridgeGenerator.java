package com.alibaba.alink.common.io.directreader;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * An factory class to create {@link DataBridge}.
 */
public interface DataBridgeGenerator {

	/**
	 * Create an data bridge.
	 * @param batchOperator the output of this batch operator will be bridged to stream job.
	 * @param params params.
	 * @return the create DataBridge object.
	 */
	DataBridge generate(BatchOperator batchOperator, Params params);
}
