package com.alibaba.alink.testutil;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import com.alibaba.alink.testutil.envfactory.EnvFactory;

import org.apache.flink.test.util.TestBaseUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.Stopwatch;
import org.junit.runner.Description;

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
            Class<?> mlEnvClz = Class.forName("com.alibaba.alink.common.MLEnvironment");
            Class<?> mlEnvFactoryClz = Class.forName("com.alibaba.alink.common.MLEnvironmentFactory");
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
}
