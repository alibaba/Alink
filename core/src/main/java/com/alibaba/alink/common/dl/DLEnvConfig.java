package com.alibaba.alink.common.dl;

import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.plugin.OsType;
import com.alibaba.alink.common.io.plugin.OsUtils;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.common.io.plugin.ResourcePluginFactory;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.alink.common.dl.DLEnvConfig.Version.TF115;
import static com.alibaba.alink.common.dl.DLEnvConfig.Version.TF231;
import static com.alibaba.alink.common.io.plugin.OsType.LINUX;
import static com.alibaba.alink.common.io.plugin.OsType.MACOSX;

public class DLEnvConfig {
	static String REGISTER_KEY_TEMPLATE = "%s_python_env_%s";
	static String PLUGIN_VERSION = "0.01";

	static Map <Pair <OsType, Version>, String> PYTHON_ENV_PATH_MAP = new HashMap <>();
	static Map <Version, String> PYTHON_ENV_KEY = new HashMap <>();

	static {
		PYTHON_ENV_PATH_MAP.put(Pair.of(LINUX, TF115),
			"https://alink-release.oss-cn-beijing.aliyuncs.com/deps-files/resources/tf115_python_env_linux-0.01/tf115-ai030-py36-linux.tar.gz");
		PYTHON_ENV_PATH_MAP.put(Pair.of(MACOSX, TF115),
			"https://alink-release.oss-cn-beijing.aliyuncs.com/deps-files/resources/tf115_python_env_macosx-0.01/tf115-ai030-py36-mac.tar.gz");
		PYTHON_ENV_PATH_MAP.put(Pair.of(LINUX, TF231),
			"https://alink-release.oss-cn-beijing.aliyuncs.com/deps-files/resources/tf231_python_env_linux-0.01/tf231-ai030-py36-linux.tar.gz");
		PYTHON_ENV_PATH_MAP.put(Pair.of(MACOSX, TF231),
			"https://alink-release.oss-cn-beijing.aliyuncs.com/deps-files/resources/tf231_python_env_macosx-0.01/tf231-ai030-py36-mac.tar.gz");

		PYTHON_ENV_KEY.put(TF115, "TF115_PYTHON_ENV_PATH");
		PYTHON_ENV_KEY.put(TF231, "TF231_PYTHON_ENV_PATH");
	}

	static RegisterKey getRegisterKey(Version version, OsType systemType) {
		return new RegisterKey(
			String.format(REGISTER_KEY_TEMPLATE, version.name().toLowerCase(), systemType.name().toLowerCase()),
			PLUGIN_VERSION
		);
	}

	public static RegisterKey getRegisterKey(Version version) {
		return getRegisterKey(version, OsUtils.getSystemType());
	}

	static String getDefaultPythonEnv(Version version) {
		String pythonEnv = null;

		// Try to get PythonEnv from environment variables
		String pythonEnvKey = PYTHON_ENV_KEY.get(version);
		if (null != pythonEnvKey) {
			pythonEnv = System.getenv(pythonEnvKey);
			if (null == pythonEnv) {
				pythonEnv = System.getProperty(pythonEnvKey);
			}
			if (null != pythonEnv) {
				if (!pythonEnv.startsWith("file://")) {
					pythonEnv = "file://" + pythonEnv;
				}
				return pythonEnv;
			}
		}

		OsType systemType = OsUtils.getSystemType();
		String remotePath = PYTHON_ENV_PATH_MAP.get(Pair.of(systemType, version));

		// Try to get PythonEnv from plugin directory
		FilePath pluginFilePath = ResourcePluginFactory.getResourcePluginPath(getRegisterKey(version));
		if (null != pluginFilePath) {
			String compressedFileName = PythonFileUtils.getCompressedFileName(remotePath);
			File directoryFile = new File(pluginFilePath.getPath().toString(), compressedFileName);
			Preconditions.checkArgument(directoryFile.exists(),
				String.format("There should be a directory named %s in plugin directory %s, but cannot be found.",
					compressedFileName, pluginFilePath));
			return "file://" + directoryFile.getAbsolutePath();
		}

		// Use default PythonEnv path in PYTHON_ENV_MAP
		if (null == remotePath) {
			throw new RuntimeException(String.format("Default python env for %s not specified.", version.name()));
		}
		return remotePath;
	}

	public static String getTF115DefaultPythonEnv() {
		return getDefaultPythonEnv(TF115);
	}

	public static String getTF231DefaultPythonEnv() {
		return getDefaultPythonEnv(TF231);
	}

	public enum Version {
		TF115,
		TF231,
		TORCH160
	}
}
