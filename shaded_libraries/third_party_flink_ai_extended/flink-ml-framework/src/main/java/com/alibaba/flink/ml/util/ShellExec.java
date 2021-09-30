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

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Helper class to run shell commands.
 */
public final class ShellExec {

	public static class ProcessLogger implements Runnable {

		private InputStream inputStream;
		private Consumer<String> consumer;

		public ProcessLogger(InputStream inputStream, Consumer<String> consumer) {
			this.inputStream = inputStream;
			this.consumer = consumer;
		}

		@Override
		public void run() {
			try {
				new BufferedReader(new InputStreamReader(inputStream)).lines().forEach(consumer);
			} catch (Exception e) {
				consumer.accept(Throwables.getStackTraceAsString(e));
			}
		}
	}

	public static class StdOutConsumer implements Consumer<String> {

		@Override
		public void accept(String s) {
			System.out.println(s);
		}
	}

	public static class StdErrorConsumer implements Consumer<String> {

		@Override
		public void accept(String s) {
			System.err.println(s);
		}
	}

	private static final Logger LOG = LoggerFactory.getLogger(ShellExec.class);
	private static final Consumer<String> dummyConsumer = s -> {
	};
	private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(1800);
	private static final ExecutorService loggerPool = new ThreadPoolExecutor(5, 20, 10,
			TimeUnit.SECONDS, new LinkedBlockingQueue<>(1024), new ThreadFactoryBuilder().setNameFormat(
			"ProcessLogger-%d").setDaemon(true).build());

	private ShellExec() {
	}

	/**
	 * Run a shell command and wait for it to complete. If a String consumer is specified, the stdout and stderr will
	 * be fed to it. And the consumer needs to be thread-safe.
	 *
	 * @param cmd the command to run
	 * @param outputConsumer a thread-safe String consumer
	 * @param timeout duration to wait for the command to complete
	 * @param allowFailure this command failure is allowed/expected
	 */
	public static boolean run(String cmd, Consumer<String> outputConsumer, Duration timeout, boolean allowFailure) {
		LOG.info("command: {}", cmd);
		ProcessBuilder builder = new ProcessBuilder("sh", "-c", cmd);
		Process process = null;
		try {
			process = builder.start();
			if (outputConsumer == null) {
				outputConsumer = dummyConsumer;
			}
			Future<?> outFuture = loggerPool.submit(new ProcessLogger(process.getInputStream(), outputConsumer));
			Future<?> errFuture = loggerPool.submit(new ProcessLogger(process.getErrorStream(), outputConsumer));
			boolean finished = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
			if (finished) {
				outFuture.get();
				errFuture.get();
			}
			boolean success = finished && process.exitValue() == 0;
			if (!success && !allowFailure) {
				if (!finished) {
					LOG.error("Command \"{}\" didn't finish in time", cmd);
				} else {
					LOG.error("Command \"{}\" failed", cmd);
				}
			}
			return success;
		} catch (IOException | ExecutionException | InterruptedException e) {
			LOG.error("Error running " + cmd, e);
			return false;
		} finally {
			if (process != null) {
				process.destroyForcibly();
			}
		}
	}

	public static boolean run(String cmd, Consumer<String> outputConsumer, Duration timeout) {
		return run(cmd, outputConsumer, timeout, false);
	}

	public static boolean run(String cmd, Consumer<String> outputConsumer, boolean allowFailure) {
		return run(cmd, outputConsumer, DEFAULT_TIMEOUT, allowFailure);
	}

	public static boolean run(String cmd, Consumer<String> outputConsumer) {
		return run(cmd, outputConsumer, DEFAULT_TIMEOUT, false);
	}

	public static boolean run(String cmd, boolean allowFailure) {
		return run(cmd, null, DEFAULT_TIMEOUT, allowFailure);
	}

	public static boolean run(String cmd) {
		return run(cmd, null, DEFAULT_TIMEOUT, false);
	}
}
