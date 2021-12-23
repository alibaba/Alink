package com.alibaba.alink.common.io.directreader;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;

import java.io.File;
import java.io.IOException;

@DataBridgeGeneratorPolicy(policy = "local_file")
public class LocalFileDataBridgeGenerator implements DataBridgeGenerator {
	@Override
	public DataBridge generate(BatchOperator <?> batchOperator, Params params) {
		File file;
		try {
			file = File.createTempFile("alink-data-bridge-", ".ak");
			Runtime.getRuntime().addShutdownHook(new Thread(() -> file.delete()));
		} catch (IOException e) {
			throw new RuntimeException("Cannot create temp file.");
		}
		new AkSinkBatchOp()
			.setFilePath(file.getAbsolutePath())
			.setOverwriteSink(true)
			.linkFrom(batchOperator);
		try {
			BatchOperator.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return new LocalFileDataBridge(file.getAbsolutePath());
	}
}
