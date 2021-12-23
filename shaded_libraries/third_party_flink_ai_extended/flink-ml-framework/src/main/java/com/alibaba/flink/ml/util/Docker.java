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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * docker cmd execute helper function
 */
public final class Docker {

	/**
	 * run a docker container builder.
	 */
	public static class ContainerBuilder {
		private String name;
		private String image;
		private String cmd;
		private Map<String, String> env = new HashMap<>(0);
		private List<String> linkHosts = new ArrayList<>(0);
		// mapping host port to container port
		private Map<Integer, Integer> mapPorts = new HashMap<>();
		private Map<String, String> volumes = new HashMap<>(0);
		private List<String> opts = new ArrayList<>(0);

		public ContainerBuilder name(String name) {
			this.name = name;
			return this;
		}

		public ContainerBuilder image(String image) {
			this.image = image;
			return this;
		}

		public ContainerBuilder cmd(String cmd) {
			this.cmd = cmd;
			return this;
		}

		public ContainerBuilder env(Map<String, String> env) {
			this.env = env;
			return this;
		}

		public ContainerBuilder env(String key, String val) {
			env.put(key, val);
			return this;
		}

		public ContainerBuilder linkHosts(List<String> linkHosts) {
			this.linkHosts = linkHosts;
			return this;
		}

		public ContainerBuilder linkHosts(String host) {
			linkHosts.add(host);
			return this;
		}

		public ContainerBuilder mapPorts(List<Integer> mapPorts) {
			for (int port : mapPorts) {
				mapPorts(port);
			}
			return this;
		}

		public ContainerBuilder mapPorts(int port) {
			return mapPorts(port, port);
		}

		public ContainerBuilder mapPorts(int hostPort, int containerPort) {
			mapPorts.put(hostPort, containerPort);
			return this;
		}

		public ContainerBuilder volumes(Map<String, String> volumes) {
			this.volumes = volumes;
			return this;
		}

		public ContainerBuilder volumes(String from, String to) {
			volumes.put(from, to);
			return this;
		}

		public ContainerBuilder opts(List<String> opts) {
			this.opts = opts;
			return this;
		}

		public ContainerBuilder opts(String opt) {
			opts.add(opt);
			return this;
		}

		public boolean build() {
			Preconditions.checkNotNull(name);
			Preconditions.checkNotNull(image);
			Preconditions.checkNotNull(cmd);

			return Docker.createAndStartContainer(name, image, cmd, env, linkHosts, mapPorts, volumes,
					opts);
		}
	}

	private static Logger LOG = LoggerFactory.getLogger(Docker.class);

	private Docker() {
	}

	private static boolean createAndStartContainer(String name, String image, String cmd,
			Map<String, String> env, List<String> linkHosts, Map<Integer, Integer> mapPorts,
			Map<String, String> volumes, List<String> opts) {
		if (containerExists(name)) {
			killAndRemoveContainer(name);
		}

		List<String> args = new ArrayList<>(opts);

		// set name
		args.add("-h " + name);
		args.add("--name " + name);

		// set env variables
		env.forEach((k, v) -> args.add(String.format("-e %s=%s", k, v)));

		// set links
		linkHosts.forEach(host -> args.add("--link " + host));

		// set volumes
		volumes.forEach((from, to) -> args.add(String.format("-v %s:%s", from, to)));

		// set ports
		mapPorts.forEach((hPort, cPort) -> args.add(String.format("-p %d:%d", hPort, cPort)));

		args.add(image);
		args.add(cmd);

		return ShellExec.run("docker run " + Joiner.on(" ").join(args), LOG::info);
	}

	/**
	 * remove docker container.
	 * @param name container name.
	 * @return true: remove container success, false: remove container failed.
	 */
	public static boolean killAndRemoveContainer(String name) {
		return ShellExec.run("docker rm -f " + name, LOG::info);
	}

	/**
	 * remove docker container.
	 * @param name container name.
	 * @return true: container exists, false: container not exists.
	 */
	public static boolean containerExists(String name) {
		StringBuffer buffer = new StringBuffer();
		Preconditions.checkState(ShellExec.run(
				"docker ps -a --format {{.Names}} --filter name=" + name, stringBufConsumer(buffer)),
				"Failed to check existing containers.");
		return buffer.toString().contains(name);
	}

	/**
	 * execute docker commend.
	 * @param container container name.
	 * @param cmd docker commend.
	 * @return true: docker commend execute success, false:docker commend execute failed.
	 */
	public static boolean execSilently(String container, String cmd) {
		return ShellExec.run(String.format("docker exec %s %s", container, cmd), true);
	}

	public static boolean exec(String container, String cmd, StringBuffer buffer) {
		return ShellExec.run(String.format("docker exec %s %s", container, cmd), stringBufConsumer(buffer));
	}

	public static boolean exec(String container, String cmd) {
		return ShellExec.run(String.format("docker exec %s %s", container, cmd), LOG::info);
	}

	public static boolean exec(String container, String cmd, Duration timeout) {
		return ShellExec.run(String.format("docker exec %s %s", container, cmd), LOG::info, timeout);
	}

	public static boolean pull(String image) {
		return ShellExec.run("docker pull " + image);
	}

	// image name should be in format "REPOSITORY:TAG"

	/**
	 * docker image exist or not.
	 * @param imageName docker image name.
	 * @return true: image exist, false: image not exists.
	 */
	public static boolean imageExist(String imageName) {
		StringBuffer buffer = new StringBuffer();
		Preconditions.checkState(ShellExec.run(
				"docker images --format {{.Repository}}:{{.Tag}}",
				stringBufConsumer(buffer)), "Failed to check existing images: " + buffer.toString());
		return buffer.toString().contains(imageName);
	}

	/**
	 * copy docker container file to local.
	 * @param container container name.
	 * @param src source address.
	 * @param dest local address.
	 * @return
	 */
	public static boolean copyFromContainer(String container, String src, String dest) {
		return ShellExec.run(String.format("docker cp %s:%s %s", container, src, dest), LOG::info);
	}

	/**
	 * copy local file to docker container.
	 * @param container container name.
	 * @param src source address.
	 * @param dest container address.
	 * @return
	 */
	public static boolean copyToContainer(String container, String src, String dest) {
		return ShellExec.run(String.format("docker cp %s %s:%s", src, container, dest), LOG::info);
	}

	private static Consumer<String> stringBufConsumer(StringBuffer buffer) {
		return s -> {
			if (buffer.length() > 0) {
				buffer.append(System.lineSeparator());
			}
			buffer.append(s);
		};
	}
}
