package com.alibaba.alink.common.dl.plugin;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.pytorch.ListSerializer;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.cluster.rpc.NodeClient;
import com.alibaba.flink.ml.cluster.rpc.RpcCode;
import com.alibaba.flink.ml.data.DataExchange;
import com.alibaba.flink.ml.proto.ContextResponse;
import com.alibaba.flink.ml.util.MLException;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.List;

/**
 * PyTorch Java API (version 1.8.1) does not provide a way to set threads directly. Intra-parallelism can only be
 * controlled by the environment variable OMP_NUM_THREADS. However, in Alink/Flink job, it is difficult to set
 * environment variables from outside, and the hack way to set inside Java process cannot work. Therefore, PyTorch
 * inference has to be started in a separated process, and config its environment variables through {@link
 * ProcessBuilder}.
 */
public class TorchJavaInferenceRunner implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(TorchJavaInferenceRunner.class);

	private final NodeClient nodeClient;
	private final MLContext mlContext;
	private final TorchJavaPredictor predictor;
	private final ListSerializer listSerializer;
	private final transient DataExchange <byte[], byte[]> dataExchange;

	TorchJavaInferenceRunner(String javaIp, int javaPort, String modelPath, Class <?>[] outputTypeClasses)
		throws MLException {
		nodeClient = new NodeClient(javaIp, javaPort);
		ContextResponse response = nodeClient.getMLContext();
		Preconditions.checkState(response.getCode() == RpcCode.OK.ordinal(), "Failed to get MLContext");
		mlContext = MLContext.fromPB(response.getContext());

		listSerializer = new ListSerializer();
		predictor = new TorchJavaPredictor(modelPath, outputTypeClasses);
		predictor.open();
		dataExchange = new DataExchange <>(mlContext);
	}

	@Override
	public void close() throws IOException {
		if (predictor != null) {
			predictor.close();
		}
		if (nodeClient != null) {
			nodeClient.close();
		}
		if (mlContext != null) {
			mlContext.close();
		}
	}

	public void run() throws IOException {
		while (true) {
			byte[] bytes;
			try {
				bytes = dataExchange.read(true);
			} catch (EOFException e) {
				break;
			}
			if (null == bytes) {
				break;
			}
			List <?> inputs = listSerializer.deserialize(bytes);
			List <?> outputs = predictor.predict(inputs);
			bytes = listSerializer.serialize(outputs);
			dataExchange.write(bytes);
		}
	}

	// args should be: ServerIP:ServerPort modelPath InputTypes InputShapes
	public static void main(String[] args) {
		LOG.info("args are {}", String.join("|", args));
		System.out.println("args are " + String.join("|", args));
		LOG.info("OMP_NUM_THREADS is {}", System.getenv("OMP_NUM_THREADS"));
		System.out.println("OMP_NUM_THREADS is " + System.getenv("OMP_NUM_THREADS"));

		String ip = args[0].split(":")[0];
		int port = Integer.parseInt(args[0].split(":")[1]);
		String modelPath = args[1];
		Class <?>[] outputTypeClasses = JsonConverter.fromJson(args[2], Class[].class);

		try {
			TorchJavaInferenceRunner runner = new TorchJavaInferenceRunner(
				ip, port, modelPath, outputTypeClasses);
			runner.run();
			runner.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		System.exit(0);
	}
}
