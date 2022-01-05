package com.alibaba.alink.testutil;

import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;

import com.alibaba.alink.testutil.envfactory.EnvFactory;
import com.google.common.collect.HashMultiset;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.Stopwatch;
import org.junit.runner.Description;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

public abstract class AlinkTestBase extends TestBaseUtils {

	private static final EnvFactory ENV_FACTORY = new EnvFactory();

	private static final Properties GLOBAL_PROPERTIES;

	protected static final Properties LOCAL_PROPERTIES = new Properties();

	/**
	 * Whether to allow local properties. If not, some common initialization methods can be merged.
	 */
	private static final boolean ALLOW_LOCAL_PROPERTIES;

	static {
		String configFilename = System.getProperty("alink.test.configFile");
		Properties properties = new Properties();
		if (null != configFilename) {
			try {
				properties.load(new FileInputStream(configFilename));
			} catch (IOException e) {
				throw new RuntimeException("Cannot read content in alink.test.configFile: " + configFilename, e);
			}
		} else {
			properties.put("envType", "local");
			properties.put("allowCustomizeProperties", "false");
			properties.put("parallelism", "2");
		}
		GLOBAL_PROPERTIES = properties;

		ALLOW_LOCAL_PROPERTIES = Boolean.parseBoolean(properties.getProperty("allowCustomizeProperties", "true"));
		if (!ALLOW_LOCAL_PROPERTIES) {
			ENV_FACTORY.initialize(properties);
		}
	}

	/**
	 * Because each test class may have different localProperties, we have to put the initialize method here.
	 * For local tests, each test class will start a mini cluster.
	 */
	@BeforeClass
	public static void beforeClass() {
		if (ALLOW_LOCAL_PROPERTIES) {
			Properties mergedConfiguration = new Properties();
			mergedConfiguration.putAll(GLOBAL_PROPERTIES);
			mergedConfiguration.putAll(LOCAL_PROPERTIES);
			ENV_FACTORY.initialize(mergedConfiguration);
		}
	}

	@AfterClass
	public static void afterClass() {
		if (ALLOW_LOCAL_PROPERTIES) {
			ENV_FACTORY.destroy();
		}
	}

	@Before
	public void before() {
		try {
			Class <?> mlEnvClz = Class.forName("com.alibaba.alink.common.MLEnvironment");
			Class <?> mlEnvFactoryClz = Class.forName("com.alibaba.alink.common.MLEnvironmentFactory");
			Method removeMethod = mlEnvFactoryClz.getMethod("remove", Long.class);
			removeMethod.invoke(null, 0L);
			Method setDefaultMethod = mlEnvFactoryClz.getMethod("setDefault", mlEnvClz);
			setDefaultMethod.invoke(null, ENV_FACTORY.getMlEnv());
		} catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	@Rule
	public Stopwatch stopwatch = new Stopwatch() {
		protected void finished(long nanos, Description description) {
			System.out.println(description.getDisplayName() + " finished, time taken " + (nanos / 1e9) + " s");
		}
	};

	/**
	 * Check equality of two lists of rows.
	 * <p>
	 * Two lists must have the same number of elements to be equal. The equality of {@link Row} depends on {@link
	 * Row#equals}. Note that floating-point numbers cannot be compared with tolerance.
	 *
	 * @param expected Expected list of rows
	 * @param actual   Actual list of rows
	 */
	public static void assertListRowEqualWithoutOrder(String message, List <Row> expected, List <Row> actual) {
		HashMultiset <Row> expectedSet = HashMultiset.create(expected);
		HashMultiset <Row> actualSet = HashMultiset.create(actual);
		Assert.assertEquals(message, expectedSet, actualSet);
	}

	public static void assertListRowEqualWithoutOrder(List <Row> expected, List <Row> actual) {
		assertListRowEqualWithoutOrder(null, expected, actual);
	}

	public static void assertRowArrayWithoutOrder(String message, Row[] expected, Row[] actual) {
		assertListRowEqualWithoutOrder(message, Arrays.asList(expected), Arrays.asList(actual));

	}

	public static void assertRowArrayWithoutOrder(Row[] expected, Row[] actual) {
		assertRowArrayWithoutOrder(null, expected, actual);
	}

	public static void assertListRowEqual(String message,
										  List <Row> expected,
										  List <Row> actual,
										  int[] sortIndices) {
		actual.sort(new Comparator <Row>() {
			@Override
			public int compare(Row row1, Row row2) {
				return AlinkTestBase.compare(row1, row2, sortIndices);
			}
		});

		Assert.assertEquals("expected size is not equal with actual size.", expected.size(), actual.size());
		for (int i = 0; i < expected.size(); i++) {
			Row expectedO = expected.get(i);
			Row actualO = actual.get(i);

			Assert.assertEquals(
				String.format("expected size is not equal with actual size. \nExpected :%s \nActual   :%s\n",
					expectedO, actualO),
				expectedO.getArity(), actualO.getArity());

			for (int j = 0; j < expectedO.getArity(); j++) {
				Assert.assertEquals(
					String.format("expected value is not equal with actual value. \nExpected :%s \nActual   :%s\n",
						expectedO, actualO),
					expectedO.getField(j), actualO.getField(j));
			}
		}
		Assert.assertArrayEquals(message, expected.toArray(), actual.toArray());
	}

	public static void assertListRowEqual(List <Row> expected,
										  List <Row> actual,
										  int[] sortIndices) {
		assertListRowEqual(null, expected, actual, sortIndices);
	}

	public static void assertListRowEqual(List <Row> expected,
										  List <Row> actual,
										  int sortIdx) {
		assertListRowEqual(null, expected, actual, new int[] {sortIdx});
	}

	private static int compare(Row row1, Row row2, int[] sortIndices) {
		for (int idx : sortIndices) {
			int compareR = AlinkTestBase.compare(row1, row2, idx);
			if (compareR != 0) {
				return compareR;
			}
		}
		return 0;
	}

	private static int compare(Row row1, Row row2, int sortIdx) {
		Object o1 = row1.getField(sortIdx);
		Object o2 = row2.getField(sortIdx);

		if (o1 == null && o2 == null) {
			return 0;
		} else if (o1 == null) {
			return 1;
		} else if (o2 == null) {
			return -1;
		}

		return ((Comparable) o1).compareTo(o2);
	}

}
