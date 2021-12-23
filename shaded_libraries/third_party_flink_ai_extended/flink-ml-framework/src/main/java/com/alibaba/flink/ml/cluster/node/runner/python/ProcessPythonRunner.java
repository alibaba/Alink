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

package com.alibaba.flink.ml.cluster.node.runner.python;

import com.alibaba.flink.ml.cluster.node.runner.AbstractScriptRunner;
import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.flink.ml.util.ShellExec;
import com.alibaba.flink.ml.util.MLException;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * python script scriptRunner,start python process to run machine learning algorithm.
 * Machine learning cluster node role has MLRunner which define how to start algorithm process.
 * Before MLRunner start algorithm process, MLRunner prepare MLContext.
 * This class define python algorithm process.
 */
public class ProcessPythonRunner extends AbstractScriptRunner {

	private static final Logger LOG = LoggerFactory.getLogger(ProcessPythonRunner.class);

	private volatile Process child = null;

	protected AtomicBoolean toKill = new AtomicBoolean(false);

	public ProcessPythonRunner(MLContext MLContext) {
		super(MLContext);
	}

	public static int checkPythonEnvironment(String cmd){
		try {
			Process process = Runtime.getRuntime().exec(cmd);
			Thread readInput = new Thread(
					new ShellExec.ProcessLogger(process.getInputStream(), new ShellExec.StdOutConsumer()));
			Thread readError = new Thread(
					new ShellExec.ProcessLogger(process.getErrorStream(), new ShellExec.StdOutConsumer()));
			readInput.start();
			readError.start();
			int cmdResult = 1;
			if (process.waitFor(5,TimeUnit.SECONDS)){
				cmdResult =  process.exitValue();
			}
			return cmdResult;
		}
		catch (Exception e){
			e.printStackTrace();
			return 1;
		}
	}

	@Override
	public void runScript() throws IOException {
		String startupScript = mlContext.getProperties().get(MLConstants.STARTUP_SCRIPT_FILE);
		List<String> args = new ArrayList<>();
		String pythonVersion = mlContext.getProperties().getOrDefault(MLConstants.PYTHON_VERSION,"");
		String pythonExec = "python" + pythonVersion;
		//check if has python2 or python3 environment
		if (checkPythonEnvironment("which " + pythonExec) != 0){
			throw new RuntimeException("No this python environment");
		}
		String virtualEnv = mlContext.getProperties()
				.getOrDefault(MLConstants.VIRTUAL_ENV_DIR, "");
		if (!virtualEnv.isEmpty()) {
			pythonExec = virtualEnv + "/bin/python";
		}
		args.add(pythonExec);
		if (mlContext.startWithStartup()) {
			args.add(startupScript);
			LOG.info("Running {} via {}", mlContext.getScript().getName(), startupScript);
		} else {
			args.add(mlContext.getScript().getAbsolutePath());
		}
		args.add(String.format("%s:%d", mlContext.getNodeServerIP(), mlContext.getNodeServerPort()));
		ProcessBuilder builder = new ProcessBuilder(args);
		String classPath = getClassPath();
		if (classPath == null) {
			// can happen in UT
			LOG.warn("Cannot find proper classpath for the Python process.");
		} else {
			mlContext.putEnvProperty(MLConstants.CLASSPATH, classPath);
		}
		buildProcessBuilder(builder);
		LOG.info("{} Python cmd: {}", mlContext.getIdentity(), Joiner.on(" ").join(args));
		runProcess(builder);
	}

	protected void runProcess(ProcessBuilder builder) throws IOException {
		child = builder.start();
		Thread inLogger = new Thread(
				new ShellExec.ProcessLogger(child.getInputStream(), new ShellExec.StdOutConsumer()));
		Thread errLogger = new Thread(
				new ShellExec.ProcessLogger(child.getErrorStream(), new ShellExec.StdOutConsumer()));
		inLogger.setName(mlContext.getIdentity() + "-in-logger");
		inLogger.setDaemon(true);
		errLogger.setName(mlContext.getIdentity() + "-err-logger");
		errLogger.setDaemon(true);
		inLogger.start();
		errLogger.start();
		try {
			int r = 0;
			do {
				if (child.waitFor(5, TimeUnit.SECONDS)) {
					r = child.exitValue();
					break;
				}
			} while (!toKill.get());

			if (r != 0) {
				throw new MLException(
						String.format("%s python process exited with code %d", mlContext.getIdentity(), r));
			}
		} catch (InterruptedException e) {
			LOG.warn("{} interrupted, killing the process", mlContext.getIdentity());
		} finally {
			killProcess();
		}
	}

	protected void buildProcessBuilder(ProcessBuilder builder) {
		String classPath;
		StringBuilder ldPath = new StringBuilder();
		String ld_path = System.getenv(MLConstants.LD_LIBRARY_PATH);
		String java_home = System.getenv(MLConstants.JAVA_HOME);
		String hdfs_home = System.getenv(MLConstants.HADOOP_HDFS_HOME);
		ldPath.append(java_home + "/jre/lib/amd64/server/:");
		ldPath.append(ld_path + ":");
		if (null != hdfs_home) {
			ldPath.append(hdfs_home + "/lib/native/:");
		}
		StringBuilder pldPath = new StringBuilder();
		String workerDir = mlContext.getProperties().get(MLConstants.WORK_DIR);
		String codePath = mlContext.getProperties().getOrDefault(MLConstants.CODE_DIR, workerDir);
		String finalPythonPath = mlContext.getProperties()
				.getOrDefault(MLConstants.ENV_PROPERTY_PREFIX + MLConstants.PYTHONPATH_ENV, "")
				+ ":" + codePath;
		mlContext.putEnvProperty(MLConstants.PYTHONPATH_ENV, finalPythonPath);

		ldPath.append(workerDir + "/tfenv/lib/:");
		for (Map.Entry<String, String> entry : mlContext.getProperties().entrySet()) {
			if (entry.getKey().startsWith(MLConstants.ENV_PROPERTY_PREFIX)) {
				String key = entry.getKey().substring(MLConstants.ENV_PROPERTY_PREFIX.length());
				if (key.equals(MLConstants.LD_LIBRARY_PATH)) {
					ldPath.append(entry.getValue()).append(":");
					continue;
				}
				LOG.info("set ENV:" + key + " " + entry.getValue());
				builder.environment().put(key, entry.getValue());
			} else if (entry.getKey().equals(MLConstants.SYS_PROPERTY_PREFIX + MLConstants.LD_LIBRARY_PATH)) {
				String[] lds = entry.getValue().split(":");
				for (String ld : lds) {
					ldPath.append(workerDir + "/tfenv/lib/" + ld).append(":");
				}
			} else if (entry.getKey().equals(MLConstants.SYS_PROPERTY_PREFIX + MLConstants.LD_PRELOAD)) {
				String[] lds = entry.getValue().split(":");
				for (String ld : lds) {
					pldPath.append(workerDir + "/tfenv/lib/" + ld).append(":");
				}
			}
		}
		if (!ldPath.toString().isEmpty()) {
			LOG.info("set ENV:" + MLConstants.LD_LIBRARY_PATH + " " + ldPath.toString());
			builder.environment().put(MLConstants.LD_LIBRARY_PATH, ldPath.toString());
		}
		if (!pldPath.toString().isEmpty()) {
			LOG.info("set ENV:" + MLConstants.LD_PRELOAD + " " + pldPath.toString());
			builder.environment().put(MLConstants.LD_PRELOAD, pldPath.toString());
		}

		classPath = builder.environment().get(MLConstants.CLASSPATH);
		if (classPath != null) {
			// remove log4j.properties from classpath, in order to avoid "log4j:ERROR setFile(null,false) call failed"
			// TODO: remove other unnecessary files as well?
			Iterable<String> elements = Splitter.on(File.pathSeparator).split(classPath);
			List<String> stripped = new ArrayList<>();
			for (String element : elements) {
				if (!StringUtils.isEmpty(element) && !element.contains("log4j.properties")) {
					stripped.add(element);
				}
			}
			builder.environment().put(MLConstants.CLASSPATH, Joiner.on(File.pathSeparator).join(stripped));
		}
	}

	private synchronized void killProcess() {
		if (child != null && child.isAlive()) {
			LOG.info("Force kill {} process", mlContext.getIdentity());
			child.destroyForcibly();
			child = null;
		}
	}

	@Override
	public void close() throws IOException {
		super.close();
		killProcess();
		LOG.info("Python scriptRunner for {} closed", mlContext.getIdentity());
	}

	public static String getClassPath() throws IOException {
		return getHadoopClassPath();
	}

	private String findContainingJar(Class cls) throws IOException {
		Preconditions.checkNotNull(cls);
		ClassLoader loader = cls.getClassLoader();
		if (loader != null) {
			String class_file = cls.getName().replaceAll("\\.", "/") + ".class";
			for (Enumeration itr = loader.getResources(class_file); itr.hasMoreElements(); ) {
				URL url = (URL) itr.nextElement();
				String path = url.getPath();
				if (path.startsWith("file:")) {
					path = path.substring("file:".length());
				}
				path = URLDecoder.decode(path, "UTF-8");
				if ("jar".equals(url.getProtocol())) {
					path = URLDecoder.decode(path, "UTF-8");
					return path.replaceAll("!.*$", "");
				}
			}
		}
		return null;
	}

	protected static String getHadoopClassPath() {
		String hadoop = findHadoopBin();
		LOG.info("HADOOP BIN:" + hadoop);
		if (hadoop == null) {
			return null;
		}
		StringBuffer buffer = new StringBuffer();
		Preconditions.checkState(ShellExec.run(hadoop + " classpath --glob", buffer::append),
				"Failed to get hadoop class path");
		return buffer.toString();
	}

	protected static String findHadoopBin() {
		String res = null;
		StringBuffer buffer = new StringBuffer();
		if (ShellExec.run("type -p hadoop", buffer::append, true)) {
			res = buffer.toString();
		} else {
			String hadoopHome = System.getenv("HADOOP_HOME");
			if (!StringUtils.isEmpty(hadoopHome)) {
				res = Joiner.on(File.separator).join(new String[] { hadoopHome, "bin", "hadoop" });
			}
		}
		if (res != null) {
			Preconditions.checkState(new File(res).exists(), res + " doesn't exist");
		}
		return res;
	}

	@Override
	public void notifyKillSignal() {
		toKill.set(true);
	}
}
