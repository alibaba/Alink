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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;

public class MiniYarnCluster {

	private static Logger LOG = LoggerFactory.getLogger(MiniYarnCluster.class);
	public static final String ZK_IMAGE = "zookeeper";
	public static final String ZK_SERVER_NAME = "minizk";
	public static final String HDFS_HOME = "/usr/local/hadoop";
	public static final int HDFS_PORT = 9000;
	private static final String CONTAINER_WORK_HOME = "/opt/work_home/";
	public static final String VENV_PACK = "tfenv.zip";
	public static final String VENV_LOCAL_PATH = CONTAINER_WORK_HOME + "/temp/test/" + VENV_PACK;
	public static final String VENV_HDFS_PATH = "/user/root/";
	public static final String YARN_NAME = "hadoop-master";
	public static final String YARN_IMAGE = "flink-yarn:v1";

	public static final String YARN_CMD = "sh -x /etc/bootstrap.sh -d";
	public static final String FLINK_HOME = "/opt/flink";
	public static final String HADOOP_HOME = HDFS_HOME;
	public static final String FLINK_CMD = FLINK_HOME + "/bin/flink";
	public static final String HADOOP_CMD = HADOOP_HOME + "/bin/hadoop";
	private static final int YARN_WEBUI_HOST_PORT = 58088;

	private String execJarPath = "";

	public void setExecJar(String jarPath) {
		this.execJarPath = jarPath;
	}


	private MiniYarnCluster() {
	}

	private static void waitHDFSReady() throws InterruptedException {
		Thread.sleep(10000);
		Preconditions.checkState(MiniCluster.mayNeedToFixAuthorizedKeys(getYarnContainer()), "Failed to start HDFS");
	}

	private static void waitClusterReady() {
		try {
			waitHDFSReady();
			while (!ShellExec.run(String.format("curl http://localhost:%d", YARN_WEBUI_HOST_PORT), true)) {
				Thread.sleep(5000);
			}
		} catch (InterruptedException e) {
			LOG.warn("Interrupted waiting for cluster to get ready", e);
		}
	}


	public static MiniYarnCluster start(boolean createEnv) {
		if (!Docker.imageExist(YARN_IMAGE)) {
			String rootPath = TestUtil.getProjectRootPath();
			String dockerDir = rootPath + "/docker/yarn";
			Preconditions.checkState(ShellExec.run(
					String.format("cd %s && docker build -t %s .", dockerDir, YARN_IMAGE), LOG::info),
					"Failed to build image: " + YARN_IMAGE);
		}
		MiniYarnCluster cluster = new MiniYarnCluster();
		try {
			Preconditions.checkState(MiniYarnCluster.startZookeeper(), "Failed to start Zookeeper");
			Preconditions.checkState(MiniYarnCluster.startYarn(), "Failed to start Yarn cluster");
			waitClusterReady();
			if (createEnv) {
				Preconditions.checkState(cluster.buildVirtualEnv(), "Failed to build virtual env");
				Preconditions.checkState(cluster.uploadVirtualEnv(), "Failed to upload virtual env");
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return cluster;
	}


	private boolean copyFromContainerToHDFS(String local, String remote) {
		return Docker.execSilently(getYarnContainer(), HADOOP_CMD + " fs -put -f " + local + " " + remote);
	}

	private boolean uploadVirtualEnv() {
		return copyFromContainerToHDFS(VENV_LOCAL_PATH, VENV_HDFS_PATH);
	}

	private boolean buildVirtualEnv() {
		String rootPath = TestUtil.getProjectRootPath();
		File tfenv = new File(rootPath + "/temp/test/tfenv.zip");
		if (tfenv.exists()) {
			return true;
		} else {
			String cmd = String.format("sh %s", CONTAINER_WORK_HOME + "docker/flink/create_venv.sh");
			return Docker.exec(getYarnContainer(), cmd);
		}
	}

	public void stop() {
		Docker.killAndRemoveContainer(getZKContainer());
		Docker.killAndRemoveContainer(getYarnContainer());
	}

	public String flinkStreamRun(String className, String... args) {
		StringBuffer buffer = new StringBuffer();
		Docker.exec(getYarnContainer(), String.format(FLINK_CMD
						+ " run -m yarn-cluster -yst -yn 2 -yjm 2048 -ytm 2048 -yD taskmanager.network.memory.max=268435456 " +
						"-c %s %s %s",
				className, uberJar(), Joiner.on(" ").join(args)), buffer);
		return buffer.toString();
	}


	public static void dumpFlinkLogs(String appId, String path) {
		final String yarnLogDir = HADOOP_HOME + "/logs/userlogs/" + appId;
		if (!Docker.copyFromContainer(getYarnContainer(), yarnLogDir, path)) {
			LOG.warn("Failed to dump logs for " + getYarnContainer());
		}
	}

	public static String parseApplicationId(String log) {
		String identify = "Submitting application master";
		String[] lines = log.split("\n");
		for (String line : lines) {
			int index = line.indexOf(identify);
			if (index > 0) {
				return line.substring(index + identify.length() + 1, line.length());
			}
		}
		return "";
	}

	private static boolean startZookeeper() {
		Docker.ContainerBuilder builder = new Docker.ContainerBuilder();
		builder.image(ZK_IMAGE).cmd("").name(getZKContainer()).opts("-d");
		return builder.build();
	}


	public static boolean startYarn() {
		LOG.info("Starting Yarn...");
//        List<Integer> ports = new ArrayList<>();
//        ports.add(50070);
//        ports.add(16666);
		Docker.ContainerBuilder builder = new Docker.ContainerBuilder();
		builder.name(getYarnContainer()).cmd(YARN_CMD).image(YARN_IMAGE);
		builder.linkHosts(getZKContainer());
		builder.opts(Collections.singletonList("-d"));
//        builder.mapPorts(ports);
		builder.mapPorts(YARN_WEBUI_HOST_PORT, 8088);
		builder.volumes(TestUtil.getProjectRootPath(), CONTAINER_WORK_HOME);
		LOG.info("Starting YARN WebUI at localhost:{}", YARN_WEBUI_HOST_PORT);
		return builder.build();
	}

	public static boolean flinkRun(String... args) {
		String cmd = Joiner.on(" ").join(args);
		return Docker.exec(getYarnContainer(), FLINK_CMD + " run " + cmd);
	}

	public static String getYarnContainer() {
		return toContainerName(YARN_NAME);
	}

	public static String getZKContainer() {
		return toContainerName(ZK_SERVER_NAME);
	}

	public static int getZKPort() {
		return 2181;
	}

	private static String toContainerName(String name) {
		// for now we only support one cluster instance
		return name;
	}

	private String uberJar() {
		return CONTAINER_WORK_HOME + execJarPath;
	}

	public static String getVenvHdfsPath() {
		return String.format("hdfs://%s:%d%s", getYarnContainer(), HDFS_PORT, VENV_HDFS_PATH + VENV_PACK);
	}

}
