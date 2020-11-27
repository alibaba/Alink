package com.alibaba.alink.common.io.directreader;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;

@DataBridgeGeneratorPolicy(policy = "memory")
public class MemoryDataBridgeGenerator implements DataBridgeGenerator {
	@Override
	public MemoryDataBridge generate(BatchOperator<?> batchOperator, Params globalParams) {
		return new MemoryDataBridge(batchOperator.collect());
	}
}
