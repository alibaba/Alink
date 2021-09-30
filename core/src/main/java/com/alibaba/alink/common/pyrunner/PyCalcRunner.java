package com.alibaba.alink.common.pyrunner;

import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.dl.DLEnvConfig;
import com.alibaba.alink.common.dl.DLEnvConfig.Version;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.common.io.plugin.ResourcePluginFactory;
import com.alibaba.alink.common.pyrunner.bridge.BasePythonBridge;
import com.alibaba.alink.common.pyrunner.bridge.DedicatedPythonBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 * A runner which calls Python code to do calculation.
 *
 * @param <IN>     input data type
 * @param <OUT>    output data type
 * @param <HANDLE> Python object handle type
 */
public abstract class PyCalcRunner<IN, OUT, HANDLE extends PyObjHandle> {

	private static final Logger LOG = LoggerFactory.getLogger(PyCalcRunner.class);

	protected final Map <String, String> config;
	private final String pythonClassName;
	private final BasePythonBridge bridge = DedicatedPythonBridge.inst();

	static final Version[] REGISTER_KEY_VERSIONS = new Version[] {Version.TF115, Version.TF231};

	protected HANDLE handle;

	public PyCalcRunner(String pythonClassName, Map <String, String> config) {
		this.pythonClassName = pythonClassName;
		this.config = config;
	}

	File extractVirtualEnv(File pluginDirectory) throws Exception {
		File[] files = pluginDirectory.listFiles((dir, name) -> name.endsWith(".tar.gz"));
		Preconditions.checkArgument(files != null && files.length == 1);
		File envFile = files[0];
		File dstDir = new File(PythonFileUtils.createTempWorkDir("python_env_"));
		TarFileUtil.unTar(envFile, dstDir);
		File[] subdirs = dstDir.listFiles(File::isDirectory);
		return null != subdirs && subdirs.length == 1 ? subdirs[0] : dstDir;
	}

	/**
	 * Start Python process if necessary and create the handle.
	 */
	public void open() {
		for (Version version : REGISTER_KEY_VERSIONS) {
			RegisterKey registerKey = DLEnvConfig.getRegisterKey(version);
			FilePath pluginFilePath = ResourcePluginFactory.getResourcePluginPath(registerKey);
			if (null != pluginFilePath) {
				File pluginDirectory = new File(pluginFilePath.getPath().getPath());
				File virtualEnvDir;
				File[] dirs = pluginDirectory.listFiles(File::isDirectory);
				Preconditions.checkArgument(null != dirs && dirs.length == 1,
					String.format("There should be only 1 directory in plugin directory: %s.", pluginDirectory));
				virtualEnvDir = dirs[0];
				config.put(BasePythonBridge.PY_VIRTUAL_ENV_KEY, virtualEnvDir.getAbsolutePath());
				LOG.info("Use virtual env in {}", virtualEnvDir.getAbsolutePath());
				break;
			}
		}
		bridge.open(getClass().getName(), config::getOrDefault, null);
		this.handle = bridge.app().newobj(pythonClassName);
	}

	public abstract OUT calc(IN in);

	/**
	 * Destroy the handle and stop Python process when necessary.
	 */
	public void close() {
		handle = null;
		bridge.close(this.getClass().getName());
	}
}
