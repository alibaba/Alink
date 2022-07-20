package com.alibaba.alink.common.dl.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.dl.DLConstants;
import com.alibaba.alink.common.dl.DLRunner;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.flink.ml.cluster.ExecutionMode;
import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.cluster.role.BaseRole;
import com.alibaba.flink.ml.cluster.rpc.NodeServer;
import com.alibaba.flink.ml.data.DataExchange;
import com.alibaba.flink.ml.util.IpHostUtil;
import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.flink.ml.util.MLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.function.Consumer;

public class DLClusterUtils {

	private static final Logger LOG = LoggerFactory.getLogger(DLClusterUtils.class);

	public static MLContext makeMLContext(int subtaskId, MLConfig config, ExecutionMode mode) {
		int numWorkers = Integer.parseInt(config.getProperties().get(DLConstants.NUM_WORKERS));
		int numPSs = Integer.parseInt(config.getProperties().get(DLConstants.NUM_PSS));	// TODO: why not used

		Tuple2 <BaseRole, Integer> roleAndIndex = DLRunner.getRoleAndIndex(subtaskId, numWorkers);
		String workDir;
		try {
			workDir = PythonFileUtils.createTempDir(String.format("temp_%d_", subtaskId)).toString();
		} catch (Exception ex) {
			throw new AkUnclassifiedErrorException("Failed to crate temporary work dir: ", ex);
		}
		config.getProperties().put(MLConstants.WORK_DIR, workDir);

		try {
			return new MLContext(mode, config, roleAndIndex.f0.name(), roleAndIndex.f1, config.getEnvPath(), null);
		} catch (MLException e) {
			throw new AkUnclassifiedErrorException("Failed to create MLContext: ", e);
		}
	}

	public static void setMLContextIpPorts(int subtaskId, MLContext mlContext, List<Tuple3 <Integer, String, Integer>> taskIpPorts) throws Exception {
		String[] ips = new String[taskIpPorts.size()];
		int[] ports = new int[taskIpPorts.size()];
		for (Tuple3<Integer, String, Integer> taskIpPort : taskIpPorts) {
			int taskId = taskIpPort.f0;
			ips[taskId] = taskIpPort.f1;
			if (subtaskId == taskId) {
				AkPreconditions.checkState(ips[taskId].equals(IpHostUtil.getIpAddress()), "task allocation changed");
			}
			ports[taskId] = taskIpPort.f2;
		}
		DLUtils.safePutProperties(mlContext, DLRunner.IPS, JsonConverter.toJson(ips));
		DLUtils.safePutProperties(mlContext, DLRunner.PORTS, JsonConverter.toJson(ports));
	}

	public static Tuple3 <DataExchange<Row, Row>, FutureTask <Void>, Thread> startDLCluster(MLContext mlContext) {
		DataExchange<Row, Row> dataExchange = new DataExchange <>(mlContext);
		FutureTask <Void> serverFuture = new FutureTask <>(new NodeServer(mlContext, mlContext.getRoleName()), null);
		Thread t = new Thread(serverFuture);
		t.setDaemon(true);
		t.setName("NodeServer_" + mlContext.getIdentity());
		t.start();
		LOG.info("start: {}, index: {}", mlContext.getRoleName(), mlContext.getIndex());
		if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
			System.out.println("start:" + mlContext.getRoleName() + " index:" + mlContext.getIndex());
		}
		return Tuple3.of(dataExchange, serverFuture, t);
	}

	public static void stopCluster(MLContext mlContext, FutureTask<Void> serverFuture, Consumer<?> preHook) {
		if (null == mlContext) {
			return;
		}
		if (mlContext.getOutputQueue() != null) {
			mlContext.getOutputQueue().markFinished();
		}
		// in ps-based training, pss do not need to wait for reading.
		if(mlContext.getRoleName() == "ps"){
			LOG.info("PS job return");
			return;
		}
		// wait for tf thread finish
		try {
			//as in batch mode, we can't user timer to drain queue, so drain it here
			preHook.accept(null);
			//drainRead(collector, true);
			if (serverFuture != null && !serverFuture.isCancelled()) {
				serverFuture.get();
			}
		} catch (InterruptedException e) {
			LOG.error("Interrupted waiting for server join {}.", e.getMessage());
			serverFuture.cancel(true);
		} catch (ExecutionException e) {
			LOG.error(mlContext.getIdentity() + " node server failed");
			throw new AkUnclassifiedErrorException(mlContext.getIdentity() + " node server failed", e);
		} finally {
			serverFuture = null;
			//long mumReadRecords = dataExchange.getReadRecords();
			int failNum = 0;

			failNum = mlContext.getFailNum();
			try {
				mlContext.close();
			} catch (IOException e) {
				LOG.error("Fail to close mlContext.", e);
			}
			mlContext = null;
			if (failNum > 0) {
				//noinspection ThrowFromFinallyBlock
				throw new AkUnclassifiedErrorException("Python script run failed, please check TaskManager logs.");
			} else {
				//LOG.info("Records output: " + mumReadRecords);
			}
		}
	}
}
