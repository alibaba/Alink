package com.alibaba.alink.common.pyrunner.fn;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.exceptions.AkParseErrorException;
import com.alibaba.alink.common.exceptions.AkPluginErrorException;
import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.common.io.plugin.ResourcePluginFactory;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.utils.UDFHelper;
import com.alibaba.alink.params.udf.BasePyBuiltInFnParams;
import com.alibaba.alink.params.udf.BasePyFileFnParams;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BuiltInFnUtils {
	private static final String PLUGIN_PREFIX = "built_in_fn_";
	private static final String CONFIG_FILE_NAME = "config.json";
	public static final String KEY_FN_PLUGIN_NAME = "fnPluginName";
	public static final String KEY_FN_PLUGIN_VERSION = "fnPluginVersion";
	public static final String KEY_FN_FACTORY_CONFIG = "factoryConfig";

	public static class BuiltInUdfConfig implements Serializable {
		public String className;
		public List <String> userFilePaths = new ArrayList <>();
		public String pythonEnvFilePath = null;
		public String pythonVersion = null;
		// result type?
	}

	public static String getPluginName(String fnName) {
		return PLUGIN_PREFIX + fnName;
	}

	public static Params downloadUpdateParams(Params params, ResourcePluginFactory factory) {
		String fnName = params.get(BasePyBuiltInFnParams.FN_NAME);
		String pluginVersion = params.get(BasePyBuiltInFnParams.PLUGIN_VERSION);
		String pluginName = getPluginName(fnName);
		RegisterKey registerKey = new RegisterKey(pluginName, pluginVersion);
		FilePath pluginPath;
		try {
			pluginPath = factory.getResourcePluginPath(registerKey);
		} catch (IOException e) {
			throw new AkPluginErrorException(
				String.format("Failed to download plugin (%s, %s).", pluginName, pluginVersion), e);
		}
		Path basePath = pluginPath.getPath();
		BaseFileSystem <?> fileSystem = pluginPath.getFileSystem();

		Path configJsonFilePath = new Path(basePath, CONFIG_FILE_NAME);
		BuiltInUdfConfig config;
		try (FSDataInputStream fis = fileSystem.open(configJsonFilePath)) {
			config = JsonConverter.gson.fromJson(new InputStreamReader(fis), BuiltInUdfConfig.class);
		} catch (IOException e) {
			throw new AkParseErrorException(
				String.format("Failed to parse built-in udf config file: %s", configJsonFilePath), e);
		}
		// Convert relative paths to absolute ones.
		params.set(BasePyFileFnParams.CLASS_NAME, config.className);
		if (null != config.pythonEnvFilePath) {
			FilePath pythonEnvFilePath = new FilePath(new Path(basePath, config.pythonEnvFilePath), fileSystem);
			params.set(BasePyFileFnParams.PYTHON_ENV_FILE_PATH, pythonEnvFilePath.serialize());
		}
		if (null != config.userFilePaths) {
			String[] serializedFilePaths = config.userFilePaths.stream()
				.map(d -> new FilePath(new Path(basePath, d), fileSystem))
				.map(FilePath::serialize)
				.toArray(String[]::new);
			params.set(BasePyFileFnParams.USER_FILE_PATHS, serializedFilePaths);
		}
		if (null != config.pythonVersion) {
			params.set(BasePyFileFnParams.PYTHON_VERSION, config.pythonVersion);
		}
		return params;
	}

	public static Tuple2 <JsonObject, Map <String, String>> downloadUpdateFnSpec(JsonObject fnSpec) {
		if (!fnSpec.has(KEY_FN_PLUGIN_NAME) || !fnSpec.has(KEY_FN_PLUGIN_VERSION) || !fnSpec.has(
			KEY_FN_FACTORY_CONFIG)) {
			return Tuple2.of(fnSpec, null);
		}
		Params params = new Params();
		params.set(BasePyBuiltInFnParams.FN_NAME, fnSpec.get(KEY_FN_PLUGIN_NAME).getAsString());
		params.set(BasePyBuiltInFnParams.PLUGIN_VERSION, fnSpec.get(KEY_FN_PLUGIN_VERSION).getAsString());
		String factorySerialized = fnSpec.get(KEY_FN_FACTORY_CONFIG).getAsString();
		ResourcePluginFactory factory = JsonConverter.fromJson(factorySerialized, ResourcePluginFactory.class);

		fnSpec.remove(KEY_FN_PLUGIN_NAME);
		fnSpec.remove(KEY_FN_PLUGIN_VERSION);
		fnSpec.remove(KEY_FN_FACTORY_CONFIG);

		final Params updatedParams = downloadUpdateParams(params, factory);
		BasePyFileFnParams <?> pyFileFnParams = () -> updatedParams;
		JsonObject updatedFnSpec = UDFHelper.makeFnSpec(pyFileFnParams);
		Map <String, String> updatedRunConfig = UDFHelper.makeRunConfig(pyFileFnParams);
		return Tuple2.of(updatedFnSpec, updatedRunConfig);
	}
}
