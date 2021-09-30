package com.alibaba.alink;

public class DLTestConstants {

	public static String LOCAL_TF115_ENV = "file:///Users/fanhong/Code/conda_tf_envs/tf115-ai030-py36-mac";
	public static String LOCAL_TF231_ENV = "file:///Users/fanhong/Code/conda_tf_envs/tf231-ai030-py36-mac";

	public static String CHN_SENTI_CORP_HTL_PATH
		= "http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/ChnSentiCorp_htl_small.csv";

	public static String BERT_CHINESE_DIR = "file:///Users/fanhong/Downloads/chinese_L-12_H-768_A-12";

	/**
	 * Download from {@link BertResources#BERT_SAVED_MODEL}.get({@link ModelName#BASE_CHINESE}).
	 */
	public static String BERT_CHINESE_SAVED_MODEL_PATH =
		"file:///Users/fanhong/Downloads/bert-base-chinese-savedmodel.tar.gz";

	static {
		if (isLinux()) {
			LOCAL_TF115_ENV = getFromPropertyOrEnv("ALINK_LOCAL_TF115_ENV");
			LOCAL_TF231_ENV = getFromPropertyOrEnv("ALINK_LOCAL_TF231_ENV");
			BERT_CHINESE_DIR = getFromPropertyOrEnv("ALINK_BERT_CHINESE_DIR");
			BERT_CHINESE_SAVED_MODEL_PATH = getFromPropertyOrEnv("ALINK_BERT_CHINESE_SAVED_MODEL_PATH",
				"http://alink-test.oss-cn-beijing.aliyuncs.com/jiqi-temp/tf_ut_files/bert-base-chinese-savedmodel.tar.gz");
		}
	}

	public static boolean isLinux() {
		String osName = System.getProperty("os.name");
		return osName.startsWith("Linux");
	}

	public static String getFromPropertyOrEnv(String key, String defaultValue) {
		if (System.getProperties().contains(key)) {
			return System.getProperty(key);
		}
		if (System.getenv().containsKey(key)) {
			return System.getenv(key);
		}
		return defaultValue;
	}

	public static String getFromPropertyOrEnv(String key) {
		return getFromPropertyOrEnv(key, null);
	}

}
