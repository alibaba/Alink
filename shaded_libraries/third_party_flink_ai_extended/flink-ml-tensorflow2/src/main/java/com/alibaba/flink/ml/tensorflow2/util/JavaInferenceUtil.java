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

package com.alibaba.flink.ml.tensorflow2.util;

import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.util.ContextService;
import com.alibaba.flink.ml.util.IpHostUtil;
import com.alibaba.flink.ml.util.ShellExec;
import com.google.common.base.Joiner;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.FutureTask;

public class JavaInferenceUtil {

	private static final Logger LOG = LoggerFactory.getLogger(JavaInferenceUtil.class);

	private JavaInferenceUtil() {
	}

	/**
	 * start a rpc server: support query machine learning context.
	 * @param mlContext machine learning node context.
	 * @return rpc server.
	 * @throws Exception
	 */
	public static Server startTFContextService(MLContext mlContext) throws Exception {
		ContextService service = new ContextService();
		Server server = ServerBuilder.forPort(0).addService(service).build();
		server.start();
		mlContext.setNodeServerIP(IpHostUtil.getIpAddress());
		mlContext.setNodeServerPort(server.getPort());
		service.setMlContext(mlContext);
		return server;
	}

	public static FutureTask<Void> startInferenceProcessWatcher(Process process, MLContext mlContext) {
		Thread inLogger = new Thread(new ShellExec.ProcessLogger(process.getInputStream(),
				new ShellExec.StdOutConsumer()));
		Thread errLogger = new Thread(new ShellExec.ProcessLogger(process.getErrorStream(),
				new ShellExec.StdOutConsumer()));
		inLogger.setName(mlContext.getIdentity() + "-JavaInferenceProcess-in-logger");
		inLogger.setDaemon(true);
		errLogger.setName(mlContext.getIdentity() + "-JavaInferenceProcess-err-logger");
		errLogger.setDaemon(true);
		inLogger.start();
		errLogger.start();
		FutureTask<Void> res = new FutureTask<>(() -> {
			try {
				int r = process.waitFor();
				inLogger.join();
				errLogger.join();
				if (r != 0) {
					throw new RuntimeException("Java inference process exited with " + r);
				}
				LOG.info("{} Java inference process finished successfully", mlContext.getIdentity());
			} catch (InterruptedException e) {
				LOG.info("{} Java inference process watcher interrupted, killing the process", mlContext.getIdentity());
			} finally {
				process.destroyForcibly();
			}
		}, null);
		Thread t = new Thread(res);
		t.setName(mlContext.getIdentity() + "-JavaInferenceWatcher");
		t.setDaemon(true);
		t.start();
		return res;
	}


	/**
	 * start tensorflow inference java process.
	 * @param mlContext machine learning node context.
	 * @param inRowType input row TypeInformation.
	 * @param outRowType output row TypeInformation.
	 * @return java inference process.
	 * @throws IOException
	 */
	public static Process launchInferenceProcess(MLContext mlContext, RowTypeInfo inRowType, RowTypeInfo outRowType)
			throws IOException {
		List<String> args = new ArrayList<>();
		String javaHome = System.getProperty("java.home");
		args.add(Joiner.on(File.separator).join(javaHome, "bin", "java"));
		// set classpath
		List<String> cpElements = new ArrayList<>();
		// add sys classpath
		cpElements.add(System.getProperty("java.class.path"));
		// add user code classpath
		if (Thread.currentThread().getContextClassLoader() instanceof URLClassLoader) {
			for (URL url : ((URLClassLoader) Thread.currentThread().getContextClassLoader()).getURLs()) {
				cpElements.add(url.toString());
			}
		}
		args.add("-cp");
		args.add(Joiner.on(File.pathSeparator).join(cpElements));
		args.add(JavaInferenceRunner.class.getCanonicalName());
		// set TF service IP & port
		args.add(String.format("%s:%d", mlContext.getNodeServerIP(), mlContext.getNodeServerPort()));
		// serialize RowType
		args.add(serializeRowType(mlContext, inRowType).toString());
		args.add(serializeRowType(mlContext, outRowType).toString());

		LOG.info("Java Inference Cmd: " + Joiner.on(" ").join(args));
		ProcessBuilder builder = new ProcessBuilder(args);
		builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
		return builder.start();
	}

	private static URI serializeRowType(MLContext mlContext, RowTypeInfo rowType) throws IOException {
		File file = mlContext.createTempFile("RowType", null);
		try (ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(file))) {
			outputStream.writeObject(rowType);
		}
		return file.toURI();
	}
}
