package com.alibaba.alink.common.pyrunner;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.flink.ml.util.MLException;
import com.alibaba.flink.ml.util.ShellExec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A utility class to run command with return code check and stdout/stderr redirection.
 */
public class ProcessBuilderRunner {
	private static final Logger LOG = LoggerFactory.getLogger(ProcessBuilderRunner.class);

	private final String identifier;

	private final ProcessBuilder processBuilder;

	private Process child;

	protected AtomicBoolean killSignal = new AtomicBoolean(false);

	public ProcessBuilderRunner(String identifier, ProcessBuilder processBuilder) {
		this.identifier = identifier;
		this.processBuilder = processBuilder;
	}

	protected void start() throws IOException {
		child = processBuilder.start();
		Thread inLogger = new Thread(
			new ShellExec.ProcessLogger(child.getInputStream(), d -> {
				LOG.info("Python stdout: {}", d);
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println(d);
				}
			}));
		Thread errLogger = new Thread(
			new ShellExec.ProcessLogger(child.getErrorStream(), d -> {
				LOG.info("Python stderr: {}", d);
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println(d);
				}
			}));
		inLogger.setName(identifier + "-in-logger");
		inLogger.setDaemon(true);
		errLogger.setName(identifier + "-err-logger");
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
			} while (!killSignal.get());
			if (r != 0) {
				throw new MLException(
					String.format("Process %s exited with code %d", identifier, r));
			}
		} catch (InterruptedException e) {
			LOG.warn("{} interrupted, killing the process", identifier);
		} finally {
			kill();
		}
	}

	private synchronized void kill() {
		if (child != null && child.isAlive()) {
			LOG.info("Force kill process {}", identifier);
			child.destroyForcibly();
			child = null;
		}
	}

	@SuppressWarnings("unused")
	public void notifyKillSignal() {
		killSignal.set(true);
	}
}
