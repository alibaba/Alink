package com.alibaba.alink.common.io.directreader;

import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.flink.ml.api.misc.param.Params;

@DataBridgeGeneratorPolicy(policy = "memory")
public class MemoryDataBridgeGenerator implements DataBridgeGenerator {
	@Override
	public MemoryDataBridge generate(BatchOperator batchOperator, Params globalParams) {
		return new MemoryDataBridge(batchOperator.collect());
	}
}
