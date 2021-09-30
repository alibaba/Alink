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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * The purpose of this class is to create Flink clusters zookeeper and HDFS as machine learning test cluster.
 */
public class MiniCluster {

	private static Logger LOG = LoggerFactory.getLogger(MiniCluster.class);
	private static final String HDFS_IMAGE = "sequenceiq/hadoop-docker";
	private static final String ZK_IMAGE = "zookeeper";
	private static final String FLINK_IMAGE = "flink-ml/flink";
	private static final String HDFS_NAME = "minidfs";
	private static final String FLINK_TM_NAME = "flink-tm";
	private static final String ZK_SERVER_NAME = "minizk";
	private static final String HDFS_CMD = "/etc/bootstrap.sh -d";
	private static final String HDFS_HOME = "/opt/hadoop-2.8.0";
	public static final String HADOOP_BIN = HDFS_HOME + "/bin/hadoop";
	private static final String FLINK_JM_NAME = "flink-jm";
	public static final String CONTAINER_WORK_HOME = "/opt/work_home/";
	private static final int HDFS_PORT = 9000;
	private static final String VENV_PACK = "tfenv.zip";
	private static final String VENV_LOCAL_PATH = CONTAINER_WORK_HOME + "/temp/test/" + VENV_PACK;
	private static final String VENV_HDFS_PATH = "/user/root/" + VENV_PACK;
	private static final Duration BUILD_SOURCE_TIMEOUT = Duration.ofMinutes(120);
	private static final int JM_WEBUI_PORT = 8081;
	private static final String FLINK_LOG_DIR = "/opt/flink/log";


	private final int numTM;
	private final long id;
	private Set<Integer> aliveContainers = new HashSet<Integer>();
	private String execJarPath = "";

	private MiniCluster(int numTM, long id) {
		this.numTM = numTM;
		this.id = id;
	}

	public void setExecJar(String jarPath) {
		this.execJarPath = jarPath;
	}

	private static void mayBuildFlinkImage() {
		if (!Docker.imageExist(FLINK_IMAGE)) {
			String rootPath = TestUtil.getProjectRootPath();
			String dockerDir = rootPath + "/docker/flink";
			Preconditions.checkState(ShellExec.run(
					String.format("cd %s && docker build -t %s:latest .", dockerDir, FLINK_IMAGE), LOG::info),
					"Failed to build image " + FLINK_IMAGE);
		}
	}

	/**
	 * start test cluster.
	 * @param numTM number of flink task manager.
	 * @return test cluster.
	 */
	public static MiniCluster start(int numTM) {
		return start(numTM, true);
	}

	/**
	 * @param numTM Number of TM to start
	 */
	public static MiniCluster start(int numTM, boolean createVenv) {
		mayBuildFlinkImage();
		MiniCluster cluster = new MiniCluster(numTM, System.currentTimeMillis());
		try {
			Preconditions.checkState(startHDFS(), "Failed to start HDFS cluster");
			Preconditions.checkState(cluster.startZookeeper(), "Failed to start Zookeeper");
			Preconditions.checkState(cluster.startFlinkJM(), "Failed to start " + FLINK_JM_NAME);
			for (int i = 0; i < numTM; i++) {
				Preconditions.checkState(cluster.startFlinkTM(i), "Failed to start " + FLINK_TM_NAME);
			}
			Preconditions.checkState(cluster.leaveSafeMode(), "NN can't leave safe mode");
			if (createVenv) {
				Preconditions.checkState(cluster.buildVirtualEnv(), "Failed to build virtual env");
				Preconditions.checkState(cluster.uploadVirtualEnv(), "Failed to upload virtual env");
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return cluster;
	}

	public void stop() {
		for (Integer i : aliveContainers) {
			Docker.killAndRemoveContainer(getTMContainer(i));
		}
		Docker.killAndRemoveContainer(getJMContainer());
		Docker.killAndRemoveContainer(getZKContainer());
		Docker.killAndRemoveContainer(getHDFSContainer());
	}

	// Kills a TM that is likely to have workload, by checking the size of log file. This is intended to be used for
	// failover tests where old logs are cleared for each job
	public int killOneTMWithWorkload() {
		int toKill = getTMWithWorkload();
		LOG.info("Killing TM: " + toKill);
		aliveContainers.remove(toKill);
		Docker.killAndRemoveContainer(getTMContainer(toKill));
		return toKill;
	}

	/**
	 * start a flink job.
	 * @param className flink job main class.
	 * @param args flink job args.
	 * @return flink job execution result.
	 */
	public String flinkRun(String className, String... args) {
		return flinkRun(className, false, args);
	}

	public String flinkRun(String className, boolean detached, String... args) {
		LOG.info("Submitting Flink job, please check progress on localhost:{}", JM_WEBUI_PORT);
		StringBuffer buffer = new StringBuffer();
		String detachArg = detached ? "-d" : "";
		Docker.exec(getJMContainer(), String.format("flink run %s -c %s %s %s",
				detachArg, className, uberJar(), Joiner.on(" ").join(args)), buffer);
		return buffer.toString();
	}

	public void dumpFlinkLogs(File dir) {
		if (!Docker.copyFromContainer(getJMContainer(), FLINK_LOG_DIR,
				new File(dir, getJMContainer() + "-log").getAbsolutePath())) {
			LOG.warn("Failed to dump logs for " + getJMContainer());
		}
		for (Integer i : aliveContainers) {
			if (!Docker.copyFromContainer(getTMContainer(i), FLINK_LOG_DIR,
					new File(dir, getTMContainer(i) + "-log").getAbsolutePath())) {
				LOG.warn("Failed to dump logs for " + getTMContainer(i));
			}
		}
	}

	private boolean startZookeeper() {
		Docker.ContainerBuilder builder = new Docker.ContainerBuilder();
		builder.image(ZK_IMAGE).cmd("").name(getZKContainer()).opts("-d");
		return builder.build();
	}

	private boolean startFlinkJM() {
		Docker.ContainerBuilder builder = flinkBuilder();
		builder.name(getJMContainer()).cmd("jobmanager");
		builder.mapPorts(8082, JM_WEBUI_PORT);
		builder.volumes(TestUtil.getProjectRootPath(), CONTAINER_WORK_HOME);
		return builder.build();
	}

	private boolean startFlinkTM(int i) {
		aliveContainers.add(i);
		Docker.ContainerBuilder builder = flinkBuilder();
		builder.name(getTMContainer(i)).cmd("taskmanager");
		builder.linkHosts(getJMContainer()).env("JOB_MANAGER_RPC_ADDRESS", getJMContainer());
		builder.env("TASK_MANAGER_NUMBER_OF_TASK_SLOTS", "2");
		return builder.build();
	}

	private static String toFlinkTmName(int index) {
		return String.format("%s-%d", FLINK_TM_NAME, index);
	}

	private Docker.ContainerBuilder flinkBuilder() {
		Docker.ContainerBuilder builder = new Docker.ContainerBuilder();
		builder.image(FLINK_IMAGE);
		builder.linkHosts(getZKContainer()).linkHosts(getHDFSContainer()).opts("-d");
		builder.opts("--ulimit core=10000000000");
		return builder;
	}

	private static boolean startHDFS() {
		LOG.info("Starting HDFS...");
		Docker.ContainerBuilder builder = new Docker.ContainerBuilder();
		builder.name(getHDFSContainer()).cmd(HDFS_CMD).image(HDFS_IMAGE);
		builder.opts(Collections.singletonList("-d"));
		boolean res = builder.build();
		if (res) {
			// make sure services are started, in order to avoid issues when we try to leave safe mode
			try {
				Thread.sleep(10000);
				res = mayNeedToFixAuthorizedKeys(getHDFSContainer());
			} catch (InterruptedException e) {
				LOG.error("Interrupted while waiting for HDFS to start.", e);
				return false;
			}
		}
		return res;
	}

	// FIXME: CI env may tamper with our authorized_keys, making HDFS/YARN fail to start. Work around it...
	static boolean mayNeedToFixAuthorizedKeys(String hadoopContainer) throws InterruptedException {
		boolean res = true;
		if (!Docker.execSilently(hadoopContainer, "sh -c 'jps | grep DataNode'")) {
			res = Docker.exec(hadoopContainer, "sh -c 'cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys'")
					&& Docker.exec(hadoopContainer, "/usr/local/hadoop/sbin/stop-dfs.sh")
					&& Docker.exec(hadoopContainer, "/usr/local/hadoop/sbin/stop-yarn.sh")
					&& Docker.exec(hadoopContainer, "/usr/local/hadoop/sbin/start-dfs.sh")
					&& Docker.exec(hadoopContainer, "/usr/local/hadoop/sbin/start-yarn.sh");
			if (res) {
				Thread.sleep(3000);
			}
		}
		return res;
	}


	private boolean leaveSafeMode() {
		// wait for NN to leave safe mode
		return Docker.execSilently(getHDFSContainer(), "/usr/local/hadoop/bin/hdfs dfsadmin -safemode wait");
	}

	private boolean uploadVirtualEnv() {
		return copyFromJMToHDFS(VENV_LOCAL_PATH, VENV_HDFS_PATH);
	}

	private boolean pipInstallTFOF(boolean buildSource) {
		String cmd = String.format("/root/setupvenv.sh %s %s",
				CONTAINER_WORK_HOME + "/core/python", String.valueOf(buildSource));
		if (buildSource) {
			LOG.info("Building TF from source code, which may take a while...");
			return Docker.exec(getJMContainer(), cmd, BUILD_SOURCE_TIMEOUT);
		}
		return Docker.exec(getJMContainer(), cmd);
	}

	private boolean buildVirtualEnv() {
		String rootPath = TestUtil.getProjectRootPath();
		File tfenv = new File(rootPath + "/temp/test/tfenv.zip");
		if (tfenv.exists()) {
			return true;
		} else {
			String cmd = String.format("bash %s", CONTAINER_WORK_HOME + "docker/flink/create_venv.sh");
			return Docker.exec(getJMContainer(), cmd);
		}
	}

	public static String getHDFSContainer() {
		return toContainerName(HDFS_NAME);
	}


	public String getZKContainer() {
		return toContainerName(ZK_SERVER_NAME);
	}

	public String getJMContainer() {
		return toContainerName(FLINK_JM_NAME);
	}

	public String getTMContainer(int i) {
		return toContainerName(toFlinkTmName(i));
	}

	private static String toContainerName(String name) {
		// for now we only support one cluster instance
		return name;
	}


	private String uberJar() {
		return CONTAINER_WORK_HOME + execJarPath;
	}

	public String getVenvHdfsPath() {
		return getHDFS() + VENV_HDFS_PATH;
	}

	public boolean copyToJM(String src, String dest) {
		return Docker.copyToContainer(getJMContainer(), src, dest);
	}

	public String getHDFS() {
		return String.format("hdfs://%s:%d", getHDFSContainer(), HDFS_PORT);
	}

	public boolean copyFromHostToHDFS(String local, String remote) {
		UUID uuid = UUID.randomUUID();
		String jmTmp = "/tmp/" + uuid;
		String name = new Path(local).getName();
		String remoteParent = new Path(remote).getParent().toString();
		return Docker.execSilently(getJMContainer(), HADOOP_BIN + " fs -mkdir -p " + remoteParent) &&
				Docker.exec(getJMContainer(), "mkdir " + jmTmp) && copyToJM(local, jmTmp) &&
				copyFromJMToHDFS(jmTmp + "/" + name, remote) && Docker.exec(getJMContainer(), "rm -rf " + jmTmp);
	}

	private boolean copyFromJMToHDFS(String local, String remote) {
		return Docker.execSilently(getJMContainer(), HADOOP_BIN + " fs -put -f " + local + " " + remote);
	}

	public void emptyTMLogs() {
		for (int i = 0; i < numTM; i++) {
			StringBuffer buffer = new StringBuffer();
			if (Docker.exec(getTMContainer(i), "ls " + FLINK_LOG_DIR, buffer)) {
				String ls = buffer.toString();
				List<String> logs = Arrays.stream(ls.split(System.lineSeparator())).map(l -> FLINK_LOG_DIR + "/" + l).
						collect(Collectors.toList());
				for (String log : logs) {
					Docker.execSilently(getTMContainer(i), String.format("sh -c \"echo > %s\"", log));
				}
			}
		}
	}

	public static String getLocalBuildDir() {
		return CONTAINER_WORK_HOME;
	}

	private int getTMWithWorkload() {
		long max = -1;
		int res = -1;
		String cmd = String.format("sh -c \"stat --printf='%%s' %s/flink-*.log\"", FLINK_LOG_DIR);
		for (int i : aliveContainers) {
			StringBuffer buffer = new StringBuffer();
			if (Docker.exec(getTMContainer(i), cmd, buffer)) {
				try {
					long size = Long.valueOf(buffer.toString());
					if (size > max) {
						max = size;
						res = i;
					}
				} catch (NumberFormatException e) {
					LOG.warn("Failed to check log size for {}: {}", getTMContainer(i), buffer.toString());
				}
			}
		}
		if (res < 0) {
			res = aliveContainers.iterator().next();
		}
		return res;
	}
}
