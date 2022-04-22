package com.alibaba.alink.common.dl.plugin;

import com.alibaba.alink.common.dl.exchange.BytesDataExchange;
import com.alibaba.alink.common.dl.plugin.DLPredictServiceMapper.PredictorConfig;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.pytorch.ListSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;

/**
 * Run a predictor in a separate process. Currently, requires usage of {@link PredictorConfig} as config.
 * <p>
 * ONNX Java API and PyTorch Java API uses OpenMP for multi-thread support. Intra-parallelism can only be controlled by
 * the environment variable OMP_NUM_THREADS. However, in Alink/Flink job, it is difficult to set environment variables
 * from outside, and the hack way to set inside Java process cannot work. Therefore, ONNX/PyTorch inference has to be
 * started in a separated process, and config its environment variables through {@link ProcessBuilder}.
 */
public class ProcessPredictorRunner implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(ProcessPredictorRunner.class);

	private final BytesDataExchange bytesDataExchange;

	private final DLPredictorService predictor;
	private final ListSerializer listSerializer;

	ProcessPredictorRunner(String predictorClassName,
						   String inQueueFilename, String outQueueFilename, PredictorConfig config)
		throws Exception {
		bytesDataExchange = new BytesDataExchange(inQueueFilename, outQueueFilename);
		listSerializer = new ListSerializer();

		Class <?> predictorClass = Class.forName(predictorClassName, true,
			Thread.currentThread().getContextClassLoader());
		assert DLPredictorService.class.isAssignableFrom(predictorClass);
		Constructor <?> constructor = predictorClass.getConstructor();
		predictor = (DLPredictorService) constructor.newInstance();
		predictor.open(config.toMap());
	}

	@Override
	public void close() throws IOException {
		if (null != bytesDataExchange) {
			bytesDataExchange.close();
		}
		if (predictor != null) {
			predictor.close();
		}
	}

	public void run() throws IOException, InterruptedException {
		while (true) {
			byte[] bytes;
			try {
				bytes = bytesDataExchange.read(true);
			} catch (EOFException e) {
				break;
			}
			if (null == bytes) {
				break;
			}
			List <?> inputs = listSerializer.deserialize(bytes);
			List <?> outputs = predictor.predict(inputs);
			bytes = listSerializer.serialize(outputs);
			bytesDataExchange.write(bytes);
		}
	}

	// args should be: predictorClassName inQueueFilename outQueueFilename configJson procReadyFilename
	// This process has to delete procReadyFilename when itself is ready.
	public static void main(String[] args) {
		for (int i = 0; i < args.length; i += 1) {
			System.out.println(String.format("arg[%d] = %s", i, args[i]));
		}
		System.out.println("OMP_NUM_THREADS is " + System.getenv("OMP_NUM_THREADS"));

		String predictorClassName = args[0];
		String inQueueFilename = args[1];
		String outQueueFilename = args[2];
		String configJson = args[3];
		String readyFilename = args[4];
		PredictorConfig config = JsonConverter.fromJson(configJson, PredictorConfig.class);
		boolean inThread = config.threadMode;

		try {
			ProcessPredictorRunner runner = new ProcessPredictorRunner(predictorClassName,
				inQueueFilename, outQueueFilename, config);
			boolean deleted = new File(readyFilename).delete();
			if (!deleted) {
				if (inThread) {
					throw new RuntimeException("Failed to create procReadyFile.");
				} else {
					System.out.println("Failed to create procReadyFile.");
					System.exit(1);
				}
			}
			runner.run();
			runner.close();
		} catch (Exception e) {
			if (inThread) {
				throw new RuntimeException("Exception caught in the inference thread:", e);
			} else {
				System.out.println("Exception caught in the inference thread:" + e);
				System.exit(1);
			}
		}
		if (!inThread) {
			System.exit(0);
		}
	}
}
