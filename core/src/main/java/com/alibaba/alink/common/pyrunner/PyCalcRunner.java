package com.alibaba.alink.common.pyrunner;

import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.dl.DLEnvConfig;
import com.alibaba.alink.common.dl.DLEnvConfig.Version;
import com.alibaba.alink.common.dl.utils.ArchivesUtils;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.common.io.plugin.ResourcePluginFactory;
import com.alibaba.alink.common.pyrunner.bridge.BasePythonBridge;
import com.alibaba.alink.common.pyrunner.bridge.DedicatedPythonBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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

	protected HANDLE handle;

	public PyCalcRunner(String pythonClassName, Map <String, String> config) {
		this.pythonClassName = pythonClassName;
		this.config = config;
	}

	File extractVirtualEnv(File pluginDirectory) throws Exception {
		File[] files = pluginDirectory.listFiles((dir, name) -> name.endsWith(".tar.gz"));
		Preconditions.checkArgument(files != null && files.length == 1);
		File envFile = files[0];
		File dstDir = PythonFileUtils.createTempDir("python_env_").toFile();
		TarFileUtil.unTar(envFile, dstDir);
		File[] subdirs = dstDir.listFiles(File::isDirectory);
		return null != subdirs && subdirs.length == 1 ? subdirs[0] : dstDir;
	}

	/**
	 * Start Python process if necessary and create the handle.
	 */
	public void open() {
		String pythonEnv = config.get(BasePythonBridge.PY_VIRTUAL_ENV_KEY);
		if (null != pythonEnv) {
			if (PythonFileUtils.isCompressedFile(pythonEnv)) {
				String tempWorkDir = PythonFileUtils.createTempDir("python_env_").toString();
				ArchivesUtils.downloadDecompressToDirectory(pythonEnv, new File(tempWorkDir));
				pythonEnv = new File(tempWorkDir, PythonFileUtils.getCompressedFileName(pythonEnv)).getAbsolutePath();
			} else {
				if (PythonFileUtils.isLocalFile(pythonEnv)) {
					pythonEnv = pythonEnv.substring("file://".length());
				}
			}
		} else {
			FilePath pluginFilePath = null;
			RegisterKey tf1RegisterKey = DLEnvConfig.getRegisterKey(Version.TF115);
			RegisterKey tf2RegisterKey = DLEnvConfig.getRegisterKey(Version.TF231);
			try {
				pluginFilePath = ResourcePluginFactory.getResourcePluginPath(tf1RegisterKey, tf2RegisterKey);
			} catch (Exception e) {
				String info = String.format("Cannot prepare plugin for %s-%s, and %s-%s, fallback to use system Python.",
					tf1RegisterKey.getName(), tf1RegisterKey.getVersion(),
					tf2RegisterKey.getName(), tf2RegisterKey.getVersion());
				LOG.info(info, e);
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println(info + ": " + e);
				}
			}
			if (null != pluginFilePath) {
				File pluginDirectory = new File(pluginFilePath.getPath().getPath());
				File[] dirs = pluginDirectory.listFiles(File::isDirectory);
				Preconditions.checkArgument(null != dirs && dirs.length == 1,
					String.format("There should be only 1 directory in plugin directory: %s.", pluginDirectory));
				pythonEnv = dirs[0].getAbsolutePath();
				LOG.info("Use virtual env in {}", pythonEnv);
			}
		}
		config.put(BasePythonBridge.PY_VIRTUAL_ENV_KEY, pythonEnv);
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
