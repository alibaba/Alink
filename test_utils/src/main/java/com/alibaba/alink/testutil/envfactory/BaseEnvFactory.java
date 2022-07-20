package com.alibaba.alink.testutil.envfactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public interface BaseEnvFactory {

    default Object makeMlEnv(ExecutionEnvironment benv, BatchTableEnvironment btenv,
                             StreamExecutionEnvironment senv, StreamTableEnvironment stenv) {
        try {
            final Class<?> mlEnvironmentClass = Class.forName("com.alibaba.alink.common.MLEnvironment");
            final Constructor<?> constructor = mlEnvironmentClass.getConstructor(ExecutionEnvironment.class, BatchTableEnvironment.class,
                    StreamExecutionEnvironment.class, StreamTableEnvironment.class);
            return constructor.newInstance(benv, btenv, senv, stenv);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException("Cannot make MLEnvironment.", e);
        }
    }

    default Configuration createConfiguration(Properties properties) {
        final Configuration configuration = new Configuration();

        final int managedMemPerCoreInMB = 64;
        final int networkMemPerCoreInMB = 64;
        final int core = Runtime.getRuntime().availableProcessors();

        Configuration conf = new Configuration();
        conf.setString(
            "taskmanager.memory.managed.size",
            String.format("%dm", managedMemPerCoreInMB * core)
        );
        conf.setString(
            "taskmanager.memory.network.min",
            String.format("%dm", networkMemPerCoreInMB * core)
        );

        final Set<String> propertyNames = properties.stringPropertyNames();
        for (String propertyName : propertyNames) {
            configuration.setString(propertyName, properties.getProperty(propertyName));
        }
        return configuration;
    }

    /**
     * Initialize factory with given properties.
     * @param properties
     */
    void initialize(Properties properties);

    /**
     * Get an MLEnvironment.
     * @return
     */
    Object getMlEnv();

    default void destroy() {}
}
