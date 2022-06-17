package com.alibaba.alink.common.dl;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.dl.DLEnvConfig.Version;
import com.alibaba.alink.common.dl.utils.DLClusterUtils;
import com.alibaba.alink.common.dl.utils.DLUtils;
import com.alibaba.alink.common.dl.utils.ExternalFilesUtils;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.io.plugin.ResourcePluginFactory;
import com.alibaba.flink.ml.cluster.ExecutionMode;
import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.data.DataExchange;
import com.alibaba.flink.ml.util.IpHostUtil;
import com.alibaba.flink.ml.util.MLConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ServerSocket;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.FutureTask;

/**
 * This co-flatmap function provides similar functions with {@link DLClusterMapPartitionFunc}, except that this one is
 * designed for stream scenario.
 * <p>
 * The following steps are performed in order:
 * <p>
 * 1. Collect IP/port information of all workers and broadcast to all workers.
 * <p>
 * 2. Prepare Python environment, and start the TF cluster.
 * <p>
 * 3. Process data stream.
 */
public class DLStreamCoFlatMapFunc extends RichCoFlatMapFunction <Row, Row, Row> {

	private static final Logger LOG = LoggerFactory.getLogger(DLStreamCoFlatMapFunc.class);

	private transient DataExchange <Row, Row> dataExchange;
	private FutureTask <Void> serverFuture;
	private volatile Collector <Row> collector = null;

	private MLContext mlContext;
	private final ResourcePluginFactory factory;
	private final MLConfig mlConfig;

	private final int numWorkers;
	private final int numPSs;
	private int taskId;

	private final List <Tuple3 <Integer, String, Integer>> taskIpPorts = new ArrayList <>();
	private boolean isTfClusterStarted = false;

	private final Queue <Row> cachedRows = new ArrayDeque <>();

	private boolean firstItem = true;

	public DLStreamCoFlatMapFunc(MLConfig mlConfig, int numWorkers, int numPSs, ResourcePluginFactory factory) {
		this.factory = factory;
		this.mlConfig = mlConfig;
		this.numWorkers = numWorkers;
		this.numPSs = numPSs;
	}

	public static void prepareExternalFiles(MLContext mlContext, String workDir) throws Exception {
		String entryFunc = mlContext.getProperties().get(DLConstants.ENTRY_FUNC);
		mlContext.setFuncName(entryFunc);
		DLUtils.safePutProperties(mlContext, DLConstants.WORK_DIR, workDir);
		workDir = new File(workDir).getAbsolutePath();

		ExternalFilesConfig externalFilesConfig =
			ExternalFilesConfig.fromJson(mlContext.getProperties().get(DLConstants.EXTERNAL_FILE_CONFIG_JSON));
		ExternalFilesUtils.prepareExternalFiles(externalFilesConfig, workDir);

		String entryScript = mlContext.getProperties().get(DLConstants.ENTRY_SCRIPT);
		String entryScriptName = PythonFileUtils.getFileName(entryScript);
		mlContext.setPythonDir(Paths.get(workDir));
		mlContext.setPythonFiles(new String[] {entryScriptName});
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.taskId = getRuntimeContext().getIndexOfThisSubtask();
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (isTfClusterStarted) {
			DLClusterUtils.stopCluster(mlContext, serverFuture, (Void) -> drainRead(collector, true));
			mlContext = null;
			isTfClusterStarted = false;
		}
	}

	@Override
	public void flatMap1(Row value, Collector <Row> out) throws Exception {
		if (!isTfClusterStarted) {
			if (firstItem) {
				ServerSocket serverSocket = IpHostUtil.getFreeSocket();
				int port = serverSocket.getLocalPort();
				serverSocket.close();
				String localIp = IpHostUtil.getIpAddress();
				for (int i = 0; i < numWorkers + numPSs; i += 1) {
					out.collect(Row.of(-1, i, Row.of(taskId, localIp, port)));
				}
				System.out.println(String.format("%d select %s:%d", taskId, localIp, port));
				//out.collect(Row.of(String.format("%d-%s-%d", taskId, localIp, port)));
				collector = out;
				firstItem = false;
			}
			if (taskId < numWorkers) { // no input for ps nodes
				cachedRows.add((Row) value.getField(1));
			}
		} else {
			if (taskId < numWorkers) {
				dataExchange.write(DLUtils.encodeStringValue((Row) value.getField(1)));
				drainRead(out, false);
			}
		}
	}

	private void startDLCluster() {
		System.out.println("Starting DL cluster...");
		try {
			mlContext = DLClusterUtils.makeMLContext(taskId, mlConfig, ExecutionMode.TRAIN);
			Map <String, String> properties = mlConfig.getProperties();
			String workDir = properties.get(MLConstants.WORK_DIR);
			DLClusterUtils.setMLContextIpPorts(taskId, mlContext, taskIpPorts);

			prepareExternalFiles(mlContext, workDir);
			// Update external files-related properties according to workDir
			{
				String pythonEnv = properties.get(DLConstants.PYTHON_ENV);
				if (StringUtils.isNullOrWhitespaceOnly(pythonEnv)) {
					Version version = Version.valueOf(properties.get(DLConstants.ENV_VERSION));
					LOG.info(String.format("Use pythonEnv from plugin: %s", version));
					pythonEnv = DLEnvConfig.getDefaultPythonEnv(factory, version);
					properties.put(MLConstants.VIRTUAL_ENV_DIR, pythonEnv.substring("file://".length()));
				} else {
					if (PythonFileUtils.isLocalFile(pythonEnv)) {
						properties.put(MLConstants.VIRTUAL_ENV_DIR, pythonEnv.substring("file://".length()));
					} else {
						properties.put(MLConstants.VIRTUAL_ENV_DIR, new File(workDir, pythonEnv).getAbsolutePath());
					}
				}
				String entryScriptFileName = PythonFileUtils.getFileName(properties.get(DLConstants.ENTRY_SCRIPT));
				mlContext.setPythonDir(new File(workDir).toPath());
				mlContext.setPythonFiles(new String[] {new File(workDir, entryScriptFileName).getAbsolutePath()});
			}

			Tuple3 <DataExchange <Row, Row>, FutureTask <Void>, Thread> dataExchangeFutureTaskThreadTuple3
				= DLClusterUtils.startDLCluster(mlContext);
			dataExchange = dataExchangeFutureTaskThreadTuple3.f0;
			serverFuture = dataExchangeFutureTaskThreadTuple3.f1;
		} catch (Exception ex) {
			throw new RuntimeException("Start TF cluster failed: ", ex);
		}
	}

	/**
	 * collect ip and port to start the cluster.
	 * @param value
	 * @param out
	 * @throws Exception
	 */
	@Override
	public void flatMap2(Row value, Collector <Row> out) throws Exception {
		value = (Row) value.getField(2);
		System.out.println(String.format("task %d received address: %s", taskId, value));
		taskIpPorts.add(
			Tuple3.of((Integer) value.getField(0), (String) value.getField(1), (Integer) value.getField(2)));
		if (taskIpPorts.size() == numWorkers + numPSs) {
			startDLCluster();
			isTfClusterStarted = true;
			System.out.println(String.format("task %d: TF cluster started", taskId));
			System.out.println(String.format("task %d: Handling %d cached rows", taskId, cachedRows.size()));
			while (!cachedRows.isEmpty()) {
				dataExchange.write(DLUtils.encodeStringValue(cachedRows.remove()));
				drainRead(out, false);
			}
		}
	}

	private void drainRead(Collector <Row> out, boolean readUntilEOF) {
		while (true) {
			try {
				Row r = dataExchange.read(readUntilEOF);
				if (r != null) {
					out.collect(Row.of(1, r));
				} else {
					break;
				}
			} catch (InterruptedIOException iioe) {
				LOG.info("{} Reading from is interrupted, canceling the server", mlContext.getIdentity());
				serverFuture.cancel(true);
			} catch (IOException e) {
				LOG.error("Fail to read data from python.", e);
				throw new RuntimeException(e);
			}
		}
	}
	//
	//@Override
	//public TypeInformation<Row> getProducedType() {
	//    return tfFlatMapFunction.getProducedType();
	//}
}
