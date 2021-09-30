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

package com.alibaba.flink.ml.util;

import com.alibaba.flink.ml.cluster.node.MLContext;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;


public class PythonUtil {

	private static final File VENV_PATH_FILE = new File(System.getProperty("java.io.tmpdir"),
			"tfof_venv_path_" + ManagementFactory.getRuntimeMXBean().getName());
	private static final File VENV_VAR_SET = new File(System.getProperty("java.io.tmpdir"),
			"tfof_venv_set_" + ManagementFactory.getRuntimeMXBean().getName());

	static {
		VENV_PATH_FILE.deleteOnExit();
		VENV_VAR_SET.deleteOnExit();
	}

	private static final Logger LOG = LoggerFactory.getLogger(PythonUtil.class);
	private static final String FIELD_SEP = "\0";

	public static void setUpVirtualEnvEmbedded(MLContext MLContext) throws Exception {
		String virtualEnv = MLContext.getEnvPath();
		if (StringUtils.isEmpty(virtualEnv)) {
			return;
		}
		deployVirtualEnv(MLContext);
		String[] paths = readFromFile();
		Preconditions.checkState(virtualEnv.equals(paths[0]), "Changing virtual env is not allowed.");
		Preconditions.checkState(new File(paths[1]).exists(), "Deployed virtual env has been removed");
		if (!VENV_VAR_SET.exists()) {
			synchronized (PythonUtil.class) {
				if (!VENV_VAR_SET.exists()) {
					String pythonPath = paths[1];
					String tfPath = pythonPath + "/com/alibaba/flink/ml";
					setPathVar(MLConstants.PYTHONPATH_ENV, pythonPath, null);
					setPathVar(MLConstants.LD_LIBRARY_PATH, tfPath, null);
					Preconditions.checkState(VENV_VAR_SET.createNewFile(),
							"Cannot create " + VENV_VAR_SET.getAbsolutePath());
				}
			}
		}
	}

	public static void setupVirtualEnvProcess(MLContext MLContext, ProcessBuilder builder) throws IOException {
		String virtualEnv = MLContext.getEnvPath();
		if (StringUtils.isEmpty(virtualEnv)) {
			return;
		}
		deployVirtualEnv(MLContext);
		String[] paths = readFromFile();
		// TODO: support different virtual env for process scriptRunner?
		Preconditions.checkState(virtualEnv.equals(readFromFile()[0]), "Changing virtual env is not allowed.");
		Preconditions.checkState(new File(paths[1]).exists(), "Deployed virtual env has been removed");
		String pythonPath = paths[1];
		setPathVar(MLConstants.PYTHONPATH_ENV, pythonPath, builder);
		String tfPath = pythonPath + "/com/alibaba/flink/ml";
		String libJvm = SystemUtils.IS_OS_MAC ? "libjvm.dylib" : "libjvm.so";
		String jvmPath = findChildByName(new File(System.getenv("JAVA_HOME")), libJvm).getParent();
		setPathVar(MLConstants.LD_LIBRARY_PATH, Joiner.on(File.pathSeparator).join(new String[] { tfPath, jvmPath }),
				builder);
	}


	public static void setupVirtualEnv(MLContext mlContext) throws IOException {
		String virtualEnv = mlContext.getEnvPath();
		LOG.info("virtualEnv:" + virtualEnv);
		if (StringUtils.isEmpty(virtualEnv)) {
			return;
		}
		String workDir = mlContext.getWorkDir().getAbsolutePath();
		String envZipName = FileUtil.parseFileName(virtualEnv);
		LOG.info("envZipName:" + envZipName);
		String envDirName = FileUtil.parseDirName(envZipName);
		LOG.info("envDirName:" + envDirName);
		File envDir = new File(workDir + "/" + envDirName);
		if (!envDir.exists()) {
			synchronized (PythonUtil.class) {
				if (!envDir.exists()) {
					LOG.info("download virtual env to local");
					FileUtil.downLoadZipToLocal(workDir, mlContext.getEnvPath(), null);
				}
			}
		}
		mlContext.getProperties().put(MLConstants.VIRTUAL_ENV_DIR, envDir.getAbsolutePath());
		String pythonPath = findChildByName(envDir, "site-packages").getAbsolutePath();
		// TODO: support different virtual env for process scriptRunner?
		String tfPath = pythonPath + "/com/alibaba/flink/ml";
		String libJvm = "libjvm.so";
		String jvmPath = findChildByName(new File(System.getenv("JAVA_HOME")), libJvm).getParent();
		mlContext.putEnvProperty(MLConstants.LD_LIBRARY_PATH,
				Joiner.on(File.pathSeparator).join(new String[] { tfPath, jvmPath }));
		mlContext.putEnvProperty(MLConstants.PYTHONPATH_ENV, pythonPath);
	}

	private static void setPathVar(String name, String value, ProcessBuilder builder) {
//        value = appendPathVar(value, builder == null ? System.getenv(name) : builder.environment().get(name));
		LOG.info("Setting {} to {}", name, value);
		if (builder != null) {
			builder.environment().put(name, value);
		} else {
			EnvVarUtil.setenv(name, value);
		}
	}

	private static String appendPathVar(String value, String current) {
		if (StringUtils.isEmpty(current)) {
			return value;
		}
		return value + File.pathSeparator + current;
	}

	private static void deployVirtualEnv(MLContext MLContext) throws IOException {
		if (!VENV_PATH_FILE.exists()) {
			synchronized (PythonUtil.class) {
				if (!VENV_PATH_FILE.exists()) {
					File localPath = downLoadEnv(MLContext);
					String pythonPath = findChildByName(localPath, "site-packages").getAbsolutePath();
					writeToFile(MLContext.getEnvPath(), pythonPath);
				}
			}
		}
	}

	private static File downLoadEnv(MLContext MLContext) throws IOException {
		Path remote = new Path(MLContext.getEnvPath());
		FileSystem fs = remote.getFileSystem(new Configuration());
		// virtual env is shared across jobs, so we can't use mlContext's temp dir here
		File tmp = Files.createTempDirectory(null).toFile();
		Runtime.getRuntime().addShutdownHook(new Thread(() -> FileUtils.deleteQuietly(tmp)));
		Path local = new Path(tmp.getPath(), remote.getName());
		LOG.info("local path:" + local.getName());
		LOG.info("remote path:" + remote.getName());
		fs.copyToLocalFile(remote, local);
		Preconditions.checkState(ShellExec.run(
				String.format("unzip -q -d %s %s", tmp.getPath(), local.toString()), LOG::info),
				"Failed to unzip virtual env");
		String name = remote.getName();
		int index = name.indexOf(".");
		if (index != -1) {
			name = name.substring(0, index);
		}
		File res = new File(tmp, name);
		if (!res.exists()) {
			res = tmp;
		}
		LOG.info("Virtual env deployed to " + res.toString());
		return res;
	}


	private static void writeToFile(String remotePath, String localPath) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(VENV_PATH_FILE))) {
			writer.write(remotePath + FIELD_SEP + localPath);
		}
	}

	private static String[] readFromFile() throws IOException {
		try (BufferedReader reader = new BufferedReader(new FileReader(VENV_PATH_FILE))) {
			return reader.readLine().split(FIELD_SEP);
		}
	}

	private static File findChildByName(File parent, String name) {
		Deque<File> deque = new ArrayDeque<>();
		deque.add(parent);
		while (!deque.isEmpty()) {
			File file = deque.remove();
			if (file.getName().equals(name)) {
				return file;
			}
			if (file.isDirectory()) {
				deque.addAll(Arrays.asList(file.listFiles()));
			}
		}
		throw new RuntimeException("Cannot find " + name + " in " + parent.getAbsolutePath());
	}

	public static File storeResourceAsFile(String resourceName, String workDir) throws IOException {
		System.out.println("resource name=" + resourceName);
		InputStream is = PythonUtil.class.getClassLoader().getResourceAsStream(resourceName);
		System.out.println("is=" + is);
		System.out.println("workDir=" + workDir);
		File targetFile = new File(workDir + "/" + resourceName);
		FileUtils.copyInputStreamToFile(is, targetFile);
		return targetFile;
	}

	public static File storeResourceAsFile(String resourceName) throws IOException {
		return storeResourceAsFile(resourceName, System.getProperty("user.dir"));
	}
}
