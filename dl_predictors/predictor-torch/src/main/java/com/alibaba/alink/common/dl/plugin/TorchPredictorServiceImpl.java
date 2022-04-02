package com.alibaba.alink.common.dl.plugin;

import com.alibaba.alink.common.dl.utils.DLUtils;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;
import com.alibaba.alink.operator.common.pytorch.ListSerializer;
import com.alibaba.flink.ml.cluster.ExecutionMode;
import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.cluster.role.WorkerRole;
import com.alibaba.flink.ml.coding.impl.ByteArrayCodingImpl;
import com.alibaba.flink.ml.data.DataExchange;
import com.alibaba.flink.ml.tensorflow2.client.DLConfig;
import com.alibaba.flink.ml.tensorflow2.util.JavaInferenceUtil;
import com.alibaba.flink.ml.util.ContextService;
import com.alibaba.flink.ml.util.IpHostUtil;
import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.flink.ml.util.MLException;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import static com.alibaba.alink.operator.common.pytorch.TorchScriptConstants.LIBRARY_PATH_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.INTRA_OP_PARALLELISM_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.MODEL_PATH_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.OUTPUT_TYPE_CLASSES;

public class TorchPredictorServiceImpl implements DLPredictorService {
	private static final Logger LOG = LoggerFactory.getLogger(TorchPredictorServiceImpl.class);

	private MLContext mlContext;
	private DataExchange <byte[], byte[]> dataExchange;
	private FutureTask <Void> futureTask;
	private Server server;

	private String modelPath;
	private String libraryPath;
	private Integer intraOpParallelism;
	private Class <?>[] outputTypeClasses;
	private ListSerializer listSerializer;

	private MLContext createMLContext() {
		if (null != mlContext) {
			return mlContext;
		}
		DLConfig config = new DLConfig(1, 0, new HashMap <>(), (String) null, null, null);
		DLUtils.safePutProperties(config, MLConstants.ENCODING_CLASS,
			ByteArrayCodingImpl.class.getCanonicalName());
		DLUtils.safePutProperties(config, MLConstants.DECODING_CLASS,
			ByteArrayCodingImpl.class.getCanonicalName());
		MLConfig mlConfig = config.getMlConfig();
		MLContext mlContext;
		try {
			mlContext = new MLContext(ExecutionMode.INFERENCE, mlConfig, new WorkerRole().name(),
				0, mlConfig.getEnvPath(), Collections.emptyMap());

		} catch (MLException e) {
			throw new RuntimeException("Cannot create MLContext", e);
		}
		return mlContext;
	}

	private void destroyMLContext(MLContext mlContext) {
		mlContext.getOutputQueue().markFinished();
		try {
			mlContext.close();
		} catch (IOException e) {
			throw new RuntimeException("Close MLContext failed.", e);
		}
	}

	private DataExchange <byte[], byte[]> createDataExchange() {
		if (null != dataExchange) {
			return dataExchange;
		}
		if (null == futureTask) {
			futureTask = createInferFutureTask();
		}
		if (null == mlContext) {
			mlContext = createMLContext();
		}
		return new DataExchange <>(mlContext);
	}

	private FutureTask <Void> createInferFutureTask() {
		if (null != futureTask) {
			return futureTask;
		}
		if (null == mlContext) {
			mlContext = createMLContext();
		}
		if (null == server) {
			server = createServer();
		}
		Process process;
		try {
			process = TorchUtils.launchInferenceProcess(
				mlContext, modelPath, outputTypeClasses, libraryPath, intraOpParallelism);
		} catch (IOException e) {
			throw new RuntimeException("Launch inference process failed.", e);
		}
		return JavaInferenceUtil.startInferenceProcessWatcher(process, mlContext);
	}

	private void destroyInferFutureTask(FutureTask <Void> inferFutureTask) {
		try {
			inferFutureTask.get();
		} catch (InterruptedException e) {
			LOG.error("Interrupted waiting for server join {}.", e.getMessage());
			inferFutureTask.cancel(true);
		} catch (ExecutionException e) {
			throw new RuntimeException("Inference process exited with exception.", e);
		}
	}

	private Server createServer() {
		if (null != server) {
			return server;
		}
		if (null == mlContext) {
			mlContext = createMLContext();
		}
		ContextService service = new ContextService();
		Server server = ServerBuilder.forPort(0).addService(service).build();
		try {
			server.start();
		} catch (IOException e) {
			throw new RuntimeException("Start MLContext service failed.", e);
		}
		try {
			mlContext.setNodeServerIP(IpHostUtil.getIpAddress());
		} catch (Exception e) {
			throw new RuntimeException("Get IP address failed.", e);
		}
		mlContext.setNodeServerPort(server.getPort());
		service.setMlContext(mlContext);
		return server;
	}

	private void destroyServer(Server server) {
		server.shutdownNow();
	}

	@Override
	public void open(Map <String, Object> config) {
		modelPath = (String) config.get(MODEL_PATH_KEY);
		libraryPath = (String) config.get(LIBRARY_PATH_KEY);
		outputTypeClasses = (Class <?>[]) config.get(OUTPUT_TYPE_CLASSES);
		intraOpParallelism = (Integer) config.get(INTRA_OP_PARALLELISM_KEY);
		listSerializer = new ListSerializer();

		try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(this.getClass().getClassLoader())) {
			mlContext = createMLContext();
			dataExchange = createDataExchange();
			futureTask = createInferFutureTask();
			server = createServer();
		}
	}

	@Override
	public void close() {
		destroyMLContext(mlContext);
		destroyInferFutureTask(futureTask);
		destroyServer(server);
		dataExchange = null;
	}

	@Override
	public List <?> predict(List <?> inputs) {
		byte[] bytes = listSerializer.serialize(inputs);
		try {
			dataExchange.write(bytes);
		} catch (IOException e) {
			throw new RuntimeException("Failed to write to data exchange.", e);
		}
		bytes = null;
		while (null == bytes) {
			try {
				if (futureTask.isDone()) {
					futureTask.get();
				}
				bytes = dataExchange.read(false);
			} catch (IOException e) {
				throw new RuntimeException("Failed to read from data exchange.", e);
			} catch (ExecutionException | InterruptedException e) {
				throw new RuntimeException("Exception thrown in inference process.", e);
			}
		}
		return listSerializer.deserialize(bytes);
	}

	@Override
	public List <List <?>> predictRows(List <List <?>> inputs, int batchSize) {
		throw new UnsupportedOperationException("Not supported batch inference yet.");
	}
}
