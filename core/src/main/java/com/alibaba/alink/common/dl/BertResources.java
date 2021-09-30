package com.alibaba.alink.common.dl;

import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.common.io.plugin.ResourcePluginFactory;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.alink.common.dl.BertResources.ModelName.BASE_CASED;
import static com.alibaba.alink.common.dl.BertResources.ModelName.BASE_CHINESE;
import static com.alibaba.alink.common.dl.BertResources.ModelName.BASE_MULTILINGUAL_CASED;
import static com.alibaba.alink.common.dl.BertResources.ModelName.BASE_UNCASED;
import static com.alibaba.alink.common.dl.BertResources.ResourceType.CKPT;
import static com.alibaba.alink.common.dl.BertResources.ResourceType.SAVED_MODEL;
import static com.alibaba.alink.common.dl.BertResources.ResourceType.VOCAB;

public class BertResources {

	private static final Map <Pair <ModelName, ResourceType>, String> BERT_RESOURCE_PATH_MAP = new HashMap <>();
	static String REGISTER_KEY_TEMPLATE = "%s_%s";
	static String PLUGIN_VERSION = "0.01";

	static {
		BERT_RESOURCE_PATH_MAP.put(Pair.of(BASE_CHINESE, VOCAB),
			"res:///tf_algos/bert/resources/bert-base-chinese-vocab.tar.gz");
		BERT_RESOURCE_PATH_MAP.put(Pair.of(BASE_MULTILINGUAL_CASED, VOCAB),
			"res:///tf_algos/bert/resources/bert-base-multilingual-cased-vocab.tar.gz");
		BERT_RESOURCE_PATH_MAP.put(Pair.of(BASE_UNCASED, VOCAB),
			"res:///tf_algos/bert/resources/bert-base-uncased-vocab.tar.gz");
		BERT_RESOURCE_PATH_MAP.put(Pair.of(BASE_CASED, VOCAB),
			"res:///tf_algos/bert/resources/bert-base-cased-vocab.tar.gz");

		BERT_RESOURCE_PATH_MAP.put(Pair.of(BASE_CHINESE, SAVED_MODEL),
			"http://pai-algo-public.oss-cn-hangzhou-zmf.aliyuncs.com/bert_files/bert-base-chinese-savedmodel.tar.gz");
		BERT_RESOURCE_PATH_MAP.put(Pair.of(BASE_MULTILINGUAL_CASED, SAVED_MODEL),
			"http://pai-algo-public.oss-cn-hangzhou-zmf.aliyuncs"
				+ ".com/bert_files/bert-base-multilingual-cased-savedmodel.tar.gz");
		BERT_RESOURCE_PATH_MAP.put(Pair.of(BASE_UNCASED, SAVED_MODEL),
			"http://pai-algo-public.oss-cn-hangzhou-zmf.aliyuncs.com/bert_files/bert-base-uncased-savedmodel.tar.gz");
		BERT_RESOURCE_PATH_MAP.put(Pair.of(BASE_CASED, SAVED_MODEL),
			"http://pai-algo-public.oss-cn-hangzhou-zmf.aliyuncs.com/bert_files/bert-base-cased-savedmodel.tar.gz");

		BERT_RESOURCE_PATH_MAP.put(Pair.of(BASE_CHINESE, CKPT),
			"http://alink-algo-packages.oss-cn-hangzhou-zmf.aliyuncs.com/bert_models/chinese_L-12_H-768_A-12.zip");
		BERT_RESOURCE_PATH_MAP.put(Pair.of(BASE_MULTILINGUAL_CASED, CKPT),
			"http://alink-algo-packages.oss-cn-hangzhou-zmf.aliyuncs.com/bert_models/multi_cased_L-12_H-768_A-12.zip");
		BERT_RESOURCE_PATH_MAP.put(Pair.of(BASE_UNCASED, CKPT),
			"http://alink-algo-packages.oss-cn-hangzhou-zmf.aliyuncs.com/bert_models/uncased_L-12_H-768_A-12.zip");
		BERT_RESOURCE_PATH_MAP.put(Pair.of(BASE_CASED, CKPT),
			"http://alink-algo-packages.oss-cn-hangzhou-zmf.aliyuncs.com/bert_models/cased_L-12_H-768_A-12.zip");
	}

	static RegisterKey getRegisterKey(ModelName modelName, ResourceType type) {
		return new RegisterKey(
			String.format(REGISTER_KEY_TEMPLATE, modelName.name().toLowerCase(), type.name().toLowerCase()),
			PLUGIN_VERSION
		);
	}

	static String getBertResource(ModelName modelName, ResourceType type) {
		String remotePath = BERT_RESOURCE_PATH_MAP.get(Pair.of(modelName, type));
		RegisterKey registerKey = getRegisterKey(modelName, type);
		FilePath pluginFilePath = ResourcePluginFactory.getResourcePluginPath(registerKey);
		if (null != pluginFilePath) {
			String directoryName = PythonFileUtils.getCompressedFileName(remotePath);
			File file = new File(pluginFilePath.getPath().toString(), directoryName);
			Preconditions.checkArgument(file.exists() && file.isDirectory(),
				String.format("There should be a directory named %s in plugin directory %s, but cannot be found.",
					directoryName, pluginFilePath.getPath().toString()));
			return "file://" + file.getAbsolutePath();
		}

		// Use default PythonEnv path in PYTHON_ENV_MAP
		if (null == remotePath) {
			throw new RuntimeException(String.format("Default resource path for %s %s not specified.",
				modelName.name(), type.name()));
		}
		System.out.println(String.format("Use plugin resource:%s", remotePath));
		return remotePath;
	}

	public static String getBertModelVocab(String name) {
		return getBertResource(ModelName.fromString(name), VOCAB);
	}

	public static String getBertSavedModel(String name) {
		return getBertResource(ModelName.fromString(name), SAVED_MODEL);
	}

	public static String getBertModelCkpt(String name) {
		return getBertResource(ModelName.fromString(name), CKPT);
	}

	public enum ModelName {
		BASE_UNCASED,
		BASE_CASED,
		BASE_MULTILINGUAL_CASED,
		BASE_CHINESE;

		static ModelName fromString(String s) {
			s = s.toUpperCase();
			s = s.replaceAll("-", "_");
			if (s.startsWith("BERT_")) {
				s = s.substring("BERT_".length());
			}
			return ModelName.valueOf(s);
		}
	}

	public enum ResourceType {
		VOCAB,
		SAVED_MODEL,
		CKPT
	}
}
