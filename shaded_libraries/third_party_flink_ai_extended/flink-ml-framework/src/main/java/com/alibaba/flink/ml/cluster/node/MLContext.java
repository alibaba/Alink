/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.ml.cluster.node;

import com.alibaba.flink.ml.cluster.ExecutionMode;
import com.alibaba.flink.ml.cluster.MLConfig;
import com.alibaba.flink.ml.proto.ColumnInfoPB;
import com.alibaba.flink.ml.proto.ContextProto;
import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.flink.ml.util.MLException;
import com.alibaba.flink.ml.util.SpscOffHeapQueue;
import com.alibaba.flink.ml.util.SysUtil;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Machine learning cluster has AM(application master) role and Node role.
 * MLContext class object is the context for am and node role to start algorithm process preparation.
 * This context contains node server address, memory queue, configuration properties, algorithm python script etc.
 * MLContext is created from MLConfig.
 * AppMasterServer, NodeServer, MLRunner, AMStateMachine etc class object  is created according to this class object.
 */
public class MLContext implements Serializable, Closeable {
	public static Logger LOG = LoggerFactory.getLogger(MLContext.class);

	private String envPath;
	//the dir to hold all user python scripts
	private Path pythonDir;
	private String[] pythonFiles;
	private String funcName;
	private String roleName;
	private int index;
	private ContextProto contextProto;

	private Map<String, Integer> roleParallelismMap;
	private Map<String, String> properties = new HashMap<>();
	private ExecutionMode mode;
	//input column names and java class types
	private final Map<String, String> inputColNameToType;

	//IPC queue mmap file; If ITC queue, null
	File inputQueueFile;
	File outputQueueFile;
	private SpscOffHeapQueue inputQueue, outputQueue;
	private SpscOffHeapQueue.QueueOutputStream outWriter;
	private SpscOffHeapQueue.QueueInputStream inReader;

	private long end;
	//queue size must be 2^n, i.e. pow(2, n)
	private final static int DEFAULT_QUEUE_SIZE = 8 * 1024 * 1024;
	private int queueSize;
	private String nodeServerIP;
	private int nodeServerPort;
	private final File localJobScratchDir = Files.createTempDir();
	private volatile boolean closed = false;

	/**
	 * @return machine learning scriptRunner failed number.
	 */
	public int getFailNum() {
		return failNum.get();
	}

	/**
	 * @param failNum machine learning scriptRunner failed number.
	 */
	public void setFailNum(int failNum) {
		this.failNum.set(failNum);
	}

	/**
	 * scriptRunner failed number add 1.
	 * @return machine learning scriptRunner failed number.
	 */
	public int addFailNum() {
		return this.failNum.addAndGet(1);
	}

	public String getMode() {
		if (mode == null) {
			return ExecutionMode.OTHER.toString();
		}
		return mode.toString();
	}

	private AtomicInteger failNum;

	/**
	 * create machine learning node runtime context.
	 * used for Java inference in separate process
	 * @param mode cluster execute mode.
	 * @param roleName node role name.
	 * @param index node index.
	 * @param roleParallelismMap cluster role parallelism information.
	 * @param funcName machine learning script main function name.
	 * @param properties context properties.
	 * @param envPath python virtual env address.
	 * @param inputColumns input information.
	 * @param inputQueueFile input memory queue file.
	 * @param outputQueueFile output memory queue file.
	 * @throws MLException
	 */
	private MLContext(ExecutionMode mode, String roleName, int index, Map<String, Integer> roleParallelismMap, String funcName,
			Map<String, String> properties, String envPath, Map<String, String> inputColumns,
			File inputQueueFile, File outputQueueFile) throws MLException {
		this.funcName = funcName;
		this.roleName = roleName;
		this.index = index;
		this.roleParallelismMap = roleParallelismMap;
		this.envPath = envPath;
		this.mode = mode;
		this.inputColNameToType = inputColumns;

		if (null != properties) {
			this.properties = properties;
		}

		queueSize = Integer
				.valueOf(this.properties.getOrDefault(MLConstants.CROSS_QUEUE_SIZE, String.valueOf(DEFAULT_QUEUE_SIZE)));
		LOG.info("set cross queue size: " + queueSize);

		failNum = new AtomicInteger(0);

		end = SysUtil.UNSAFE.allocateMemory(MLConstants.INT_SIZE);
		SysUtil.UNSAFE.setMemory(end, MLConstants.INT_SIZE, (byte) MLConstants.END_STATUS_NORMAL);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> FileUtils.deleteQuietly(localJobScratchDir)));
		createQueue(inputQueueFile, outputQueueFile, false);
	}

	/**
	 * create machine learning node runtime context.
	 * used for Java inference in separate process
	 * @param mode cluster execute mode.
	 * @param roleName node role name.
	 * @param index node index.
	 * @param envPath python virtual env address.
	 * @param inputColumns input information.
	 * @throws MLException
	 */
	public MLContext(ExecutionMode mode, MLConfig mlConfig, String roleName, int index, String envPath,
			Map<String, String> inputColumns) throws MLException {
		this(mode, roleName, index, mlConfig.getRoleParallelismMap(), mlConfig.getFuncName(),
				mlConfig.getProperties(), envPath, inputColumns);
	}

	/**
	 * create machine learning node runtime context.
	 * used for Java inference in separate process
	 * @param mode cluster execute mode.
	 * @param roleName node role name.
	 * @param index node index.
	 * @param roleParallelismMap cluster role parallelism information.
	 * @param funcName machine learning script main function name.
	 * @param properties context properties.
	 * @param envPath python virtual env address.
	 * @param inputColumns input information.
	 * @throws MLException
	 */
	public MLContext(ExecutionMode mode, String roleName, int index, Map<String, Integer> roleParallelismMap, String funcName,
			Map<String, String> properties, String envPath, Map<String, String> inputColumns) throws MLException {
		this.funcName = funcName;
		this.roleName = roleName;
		this.index = index;
		this.roleParallelismMap = roleParallelismMap;
		this.envPath = envPath;
		this.mode = mode;
		this.inputColNameToType = inputColumns;

		if (null != properties) {
			this.properties = properties;
		}

		queueSize = Integer
				.valueOf(this.properties.getOrDefault(MLConstants.CROSS_QUEUE_SIZE, String.valueOf(DEFAULT_QUEUE_SIZE)));
		LOG.info("set cross queue size: " + queueSize);

		failNum = new AtomicInteger(0);

		end = SysUtil.UNSAFE.allocateMemory(MLConstants.INT_SIZE);
		SysUtil.UNSAFE.setMemory(end, MLConstants.INT_SIZE, (byte) MLConstants.END_STATUS_NORMAL);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> FileUtils.deleteQuietly(localJobScratchDir)));
		createQueue();
	}

	private void createQueue() throws MLException {
		try {
			File inFile = createTempFile("queue-", ".input");
			File outFile = createTempFile("queue-", ".output");
			createQueue(inFile, outFile, true);
		} catch (IOException e) {
			throw new MLException("Fail to create queue", e);
		}

	}

	// Queues are originally created on flink side and then used to communicate with other Python/Java processes.
	// So reset should only be true when the queues are created in flink.
	private void createQueue(File inputQueueFile, File outputQueueFile, boolean reset)
			throws MLException {
		try {
			this.inputQueueFile = inputQueueFile;
			this.outputQueueFile = outputQueueFile;
			inputQueue = new SpscOffHeapQueue(inputQueueFile.getAbsolutePath(), queueSize, reset);
			outputQueue = new SpscOffHeapQueue(outputQueueFile.getAbsolutePath(), queueSize, reset);
			outWriter = new SpscOffHeapQueue.QueueOutputStream(outputQueue);
			inReader = new SpscOffHeapQueue.QueueInputStream(inputQueue);
		} catch (Exception e) {
			throw new MLException("Fail to create queue", e);
		}
	}

	/**
	 * clear node context resources.
	 * @throws IOException
	 */
	@Override
	public synchronized void close() throws IOException {
		LOG.info("{} closing mlContext", getIdentity());
		if (inputQueue != null) {
			inputQueue.close();
			inputQueue = null;
		}

		if (outputQueue != null) {
			outputQueue.close();
			outputQueue = null;
		}

		if (inputQueueFile != null) {
			inputQueueFile.delete();
			inputQueueFile = null;
		}

		if (outputQueueFile != null) {
			outputQueueFile.delete();
			outputQueueFile = null;
		}


		SysUtil.UNSAFE.freeMemory(end);
		closed = true;
		FileUtils.deleteQuietly(localJobScratchDir);
	}

	/**
	 * @return machine learning run script path.
	 */
	public File getScript() {
		if (pythonDir == null || pythonFiles == null || pythonFiles.length == 0) {
			return null;
		}
		return new File(pythonDir.toFile(), pythonFiles[0]);
	}

	private String getScriptPath() {
		File script = getScript();
		return script != null ? script.getAbsolutePath() : "";
	}

	/**
	 * @return python virtual environment.
	 */
	public String getEnvPath() {
		return envPath;
	}

	/**
	 * set python virtual environment path.
	 * @param envPath virtual environment path.
	 */
	public void setEnvPath(String envPath) {
		this.envPath = envPath;
	}


	public Map<String, String> getProperties() {
		return properties;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public String getRoleName() {
		return roleName;
	}

	public void setFuncName(String funcName) {
		this.funcName = funcName;
	}

	public SpscOffHeapQueue getInputQueue() {
		return inputQueue;
	}


	public SpscOffHeapQueue.QueueOutputStream getOutWriter() {
		return outWriter;
	}

	public SpscOffHeapQueue.QueueInputStream getInReader() {
		return inReader;
	}

	public SpscOffHeapQueue getOutputQueue() {
		return outputQueue;
	}

	public String getFuncName() {
		return funcName;
	}

	@Override
	public String toString() {
		return "mlContext{" +
				"virtual env path='" + envPath + '\'' +
				", pythonDir=" + pythonDir +
				", pythonFiles=" + Arrays.toString(pythonFiles) +
				", funcName='" + funcName + '\'' +
				", roleName='" + roleName + '\'' +
				", index=" + index +
				", properties=" + properties +
				", mode=" + mode +
				'}';
	}


	/**
	 * get context end flag.
	 * @return end flag.
	 */
	public int getEnd() {
		return SysUtil.UNSAFE.getInt(end);
	}

	/**
	 * set context end flag.
	 * @param v end flag
	 */
	public void setEnd(int v) {
		SysUtil.UNSAFE.putInt(this.end, v);
	}

	/**
	 * reset node context.
	 */
	public synchronized void reset() {
		if (closed) {
			// if mlContext is closed the memory has been freed
			LOG.info("{} not resetting mlContext as it's already closed", getIdentity());
			return;
		}
		LOG.info("{} reset mlContext", getIdentity());
		SysUtil.UNSAFE.putInt(this.end, MLConstants.END_STATUS_NORMAL);
		if (inputQueue != null) {
			inputQueue.reset();
		}
		if (outputQueue != null) {
			outputQueue.reset();
		}
	}

	public List<String> getPythonFiles() {
		return Arrays.asList(pythonFiles);
	}

	public void setPythonFiles(String[] pythonFiles) {
		this.pythonFiles = pythonFiles;
	}

	public Path getPythonDir() {
		return pythonDir;
	}

	public void setPythonDir(Path pythonDir) {
		this.pythonDir = pythonDir;
	}

	public void println(String str) {
		System.out.println(str);
	}

	public String getIdentity() {
		return roleName + ":" + String.valueOf(index);
	}

	public String getNodeServerIP() {
		return nodeServerIP;
	}

	public void setNodeServerIP(String nodeServerIP) {
		this.nodeServerIP = nodeServerIP;
	}

	public int getNodeServerPort() {
		return nodeServerPort;
	}

	public void setNodeServerPort(int nodeServerPort) {
		this.nodeServerPort = nodeServerPort;
	}


	/**
	 * used for Java inference in separate process, create MLContext from ContextProto.
	 * @param pb ContextProto.
	 * @return MLContext.
	 * @throws MLException
	 */
	public static MLContext fromPB(ContextProto pb) throws MLException {
		// currently only support IPC
		ExecutionMode mode = ExecutionMode.valueOf(pb.getMode());
		String roleName = pb.getRoleName();
		int index = pb.getIndex();
		Map<String, Integer> jobNumMap = pb.getRoleParallelismMap();
		String funcName = pb.getFuncName();
		// Map<String, String> props = pb.getPropsMap();
		Map<String, String> props = new HashMap<>();
		props.putAll(pb.getPropsMap());
		// TODO: add envPath?
		Map<String, String> inputCols = new HashMap<>();
		for (ColumnInfoPB colInfo : pb.getColumnInfosList()) {
			inputCols.put(colInfo.getName(), colInfo.getType());
		}
		// the input/output queues have already been flipped in toPB
		File inQueueFile = new File(pb.getInQueueName());
		File outQueueFile = new File(pb.getOutQueueName());
		// no need to specify QueueMMapLen since it'll be computed based on the queue size
		return new MLContext(mode, roleName, index, jobNumMap, funcName, props,
				null, inputCols, inQueueFile, outQueueFile);
	}

	/**
	 * convert MLContext to context proto.
	 * @return context proto.
	 */
	public ContextProto toPB() {
		ContextProto.Builder builder = toPBBuilder();
		return builder.build();
	}

	/**
	 * convert MLContext to proto builder.
	 * @return ContextProto.Builder
	 */
	public ContextProto.Builder toPBBuilder() {
		ContextProto.Builder builder = ContextProto.newBuilder();
		builder.setMode(getMode()).setFailNum(getFailNum()).setRoleName(getRoleName());
		String funcName = getFuncName() == null ? "" : getFuncName();
		builder.setIndex(getIndex()).setFuncName(funcName).setIdentity(getIdentity());
		if (outputQueueFile != null) {
			builder.setOutQueueMMapLen(inputQueue.getMmapLen());
			builder.setOutQueueName(inputQueueFile.getAbsolutePath());
			builder.setInQueueMMapLen(outputQueue.getMmapLen());
			builder.setInQueueName(outputQueueFile.getAbsolutePath());
		}
		builder.putAllProps(getProperties()).setUserScript(getScriptPath());
		if (inputColNameToType != null) {
			for (String name : inputColNameToType.keySet()) {
				builder.addColumnInfos(
						ColumnInfoPB.newBuilder().setName(name).setType(inputColNameToType.get(name)).build());
			}
		}
		builder.putAllRoleParallelism(roleParallelismMap);
		return builder;
	}

	public File createTempFile(String name) throws IOException {
		File f = new File(localJobScratchDir, name);
		Preconditions.checkState(f.createNewFile(), "Failed to create file " + name);
		return f;
	}

	public File createTempDir() throws IOException {
		return createTempDir(null).toFile();
	}

	public File createTempFile(String prefix, String suffix) throws IOException {
		return File.createTempFile(prefix, suffix, localJobScratchDir);
	}

	public Path createTempDir(String prefix, FileAttribute<?>... attrs) throws IOException {
		return java.nio.file.Files.createTempDirectory(localJobScratchDir.toPath(), prefix, attrs);
	}

	public File getWorkDir() {
		String workDirStr = getProperties().get(MLConstants.WORK_DIR);
		if (workDirStr != null && new File(workDirStr).exists()) {
			return new File(workDirStr);
		}

		File workDir = new File(System.getProperty("user.dir"));
		workDir = new File(workDir, "temp");
		if (!workDir.exists()) {
			workDir.mkdirs();
		}
		getProperties().put(MLConstants.WORK_DIR, workDir.getAbsolutePath());
		return workDir;
	}

	public Map<String, Integer> getRoleParallelismMap() {
		return roleParallelismMap;
	}

	public void setRoleParallelismMap(Map<String, Integer> roleParallelismMap) {
		this.roleParallelismMap = roleParallelismMap;
	}


	public void putEnvProperty(String key, String value) {
		this.properties.put("ENV:" + key, value);
	}

	public String getEnvProperty(String key) {
		return this.properties.getOrDefault("ENV:" + key, "");
	}

	public boolean startWithStartup() {
		if (this.properties.containsKey(MLConstants.START_WITH_STARTUP)) {
			if (this.properties.get(MLConstants.START_WITH_STARTUP).equalsIgnoreCase("false")) {
				return false;
			} else {
				return true;
			}
		} else {
			return true;
		}
	}

	public boolean useDistributeCache() {
		if (this.properties.containsKey(MLConstants.USE_DISTRIBUTE_CACHE)) {
			if (this.properties.get(MLConstants.USE_DISTRIBUTE_CACHE).equalsIgnoreCase("false")) {
				return false;
			} else {
				return true;
			}
		} else {
			if (this.properties.getOrDefault(MLConstants.REMOTE_CODE_ZIP_FILE, "").isEmpty()) {
				return true;
			} else {
				return false;
			}
		}
	}

	public List<String> getHookClassNames() {
		String hooks = this.getProperties().getOrDefault(MLConstants.FLINK_HOOK_CLASSNAMES, "");
		List<String> hookList = new ArrayList<>();
		if (!hooks.isEmpty()) {
			String[] hookNames = hooks.split(MLConstants.SEPERATOR_COMMA);
			for (String h : hookNames) {
				hookList.add(h);
			}
		}
		return hookList;
	}

	public boolean isBatchMode() {
		return !isStreamMode();
	}

	public boolean isStreamMode() {
		return Boolean.valueOf(this.properties.getOrDefault(MLConstants.CONFIG_JOB_HAS_INPUT, "false"));
	}


	public ContextProto getContextProto() {
		return contextProto;
	}

	/**
	 * set context proto, node can rewrite context proto.
	 * @param contextProto
	 */
	public void setContextProto(ContextProto contextProto) {
		this.contextProto = contextProto;
	}
}
