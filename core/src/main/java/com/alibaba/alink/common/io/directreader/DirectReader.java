package com.alibaba.alink.common.io.directreader;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.operator.batch.BatchOperator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;

/**
 * DirectReader is used to get the corresponding {@link DataBridge} from batch operator. The type of {@link DataBridge}
 * created is depending on configuration. Configurations comes from the following three sources with DESCENDING
 * priority:
 * <ul>
 *     <li>1. User specified via {@link DirectReaderPropertiesStore#setProperties(Properties)}.</li>
 *     <li>2. System properties, saying: <code>-Ddirect.reader.policy=db</code>. </li>
 *     <li>3. Content of a file named direct_reader.properties on class path.</li>
 * </ul>
 *
 * The default policy is memory.
 * <p>
 *
 * An configuration example:
 * <pre>
 * direct.reader.policy = db
 * direct.reader.db.key0 = value0
 * direct.reader.db.key1 = value1
 * </pre>
 */
public class DirectReader implements Serializable {
	private final static String DIRECT_READER_PREFIX = "direct.reader";
	private final static String DIRECT_READER_CONFIG_FILE_PATH = "direct_reader.properties";

	private final static ParamInfo <String> POLICY_KEY = ParamInfoFactory
		.createParamInfo("policy", String.class)
		.setDescription("policy of direct reader")
		.setRequired()
		.build();

	/**
	 * Create data bridge from batch operator.
	 * The type of result DataBridge is the one with matching policy in global configuration.
	 *
	 *
	 * @param model the operator to collect data.
	 * @return the created DataBridge.
	 */
	public static DataBridge collect(BatchOperator<?> model) {
		final Params globalParams = DirectReader.readProperties();
		final String policy = globalParams.get(POLICY_KEY);

		for (DataBridgeGenerator generator : ServiceLoader.load(DataBridgeGenerator.class, DirectReader.class.getClassLoader())) {
			if (policy.equals(generator
				.getClass()
				.getAnnotation(DataBridgeGeneratorPolicy.class)
				.policy()
			)) {
				return generator.generate(model, globalParams);
			}
		}

		throw new IllegalArgumentException("Can not find the policy: " + policy);
	}

	/**
	 * Read data from a BatchOperator by creating an internal DirectBridge on that operator.
	 *
	 * @param batchOperator data source
	 * @return Row list.
	 */
	public static List <Row> directRead(BatchOperator batchOperator) {
		return directRead(collect(batchOperator));
	}

	/**
	 * Read all data from DataBridge.
	 *
	 * @param dataBridge reading source.
	 * @return Row list.
	 */
	public static List<Row> directRead(DataBridge dataBridge) {
		return dataBridge.read(null);
	}

	/**
	 * Filter data from DataBridge, rows with a true value from filter will be reserved.
	 *
	 * @param dataBridge data source
	 * @param filter     the filter
	 * @return filtered row list.
	 */
	public static List <Row> directRead(DataBridge dataBridge, FilterFunction <Row> filter) {
		return dataBridge.read(filter);
	}

	private static Properties filterProperties(Properties properties) {
		Properties filtered = new Properties();

		for (String name : properties.stringPropertyNames()) {
			if (name.startsWith(DIRECT_READER_PREFIX)) {
				filtered.put(
					name.substring(DIRECT_READER_PREFIX.length() + 1),
					properties.getProperty(name)
				);
			}
		}

		return filtered;
	}

	private static Params properties2Params(Properties properties) {
		if (properties.containsKey(POLICY_KEY.getName())) {
			String policy = properties.getProperty(POLICY_KEY.getName());
			Params params = new Params().set(POLICY_KEY, policy);

			for (String name : properties.stringPropertyNames()) {
				if (name.startsWith(policy)) {
					params.set(
						AnnotationUtils.dynamicParamKey(name.substring(policy.length() + 1)),
						properties.getProperty(name)
					);
				}
			}

			return params;
		} else {
			throw new RuntimeException("Error properties. it has not policy key");
		}
	}

	private static Params readProperties() {
		Properties fileProperties = new Properties();

		InputStream inputStream = null;
		if (Files.exists(Paths.get(DIRECT_READER_CONFIG_FILE_PATH))) {
			// load local configuration file
			try {
				inputStream = new FileInputStream(DIRECT_READER_CONFIG_FILE_PATH);
			} catch (FileNotFoundException e) {
				//pass
			}
		} else {
			// load from file in classpath.
			inputStream = Thread
				.currentThread()
				.getContextClassLoader()
				.getResourceAsStream(DIRECT_READER_CONFIG_FILE_PATH);
		}

		if (inputStream != null) {
			try {
				fileProperties.load(inputStream);
			} catch (IOException e) {
				//pass
			}
		}

		Properties defaultProperties = new Properties();

		defaultProperties.put(POLICY_KEY.getName(),
			MemoryDataBridgeGenerator.class.getAnnotation(DataBridgeGeneratorPolicy.class).policy()
		);

		defaultProperties.putAll(
			filterProperties(fileProperties)
		);

		defaultProperties.putAll(
			filterProperties(System.getProperties())
		);

		defaultProperties.putAll(
			filterProperties(DirectReaderPropertiesStore.getProperties())
		);

		return properties2Params(defaultProperties);
	}
}
