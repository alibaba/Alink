package com.alibaba.alink.testutil.envfactory.impl;

import java.util.Properties;

import com.alibaba.alink.testutil.envfactory.BaseEnvFactory;
import com.alibaba.alink.testutil.envfactory.EnvTypeAnnotation;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.test.util.TestEnvironment;

@EnvTypeAnnotation(name = "local")
public class LocalEnvFactoryImpl implements BaseEnvFactory {
    private MiniClusterResource miniClusterResource;

    @Override
    public void initialize(Properties properties) {
        final int parallelism = Integer.parseInt(properties.getProperty("parallelism", "2"));
        final Configuration configuration = createConfiguration(properties);
        miniClusterResource = new MiniClusterResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setConfiguration(configuration)
                        .setNumberTaskManagers(parallelism)
                        .setNumberSlotsPerTaskManager(1)
                        .build());
        try {
            miniClusterResource.before();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        TestEnvironment.setAsContext(miniClusterResource.getMiniCluster(), miniClusterResource.getNumberSlots());
        TestStreamEnvironment.setAsContext(miniClusterResource.getMiniCluster(), miniClusterResource.getNumberSlots());
    }

    @Override
    public Object getMlEnv() {
        ExecutionEnvironment benv = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment btenv = BatchTableEnvironment.create(benv);
        StreamTableEnvironment stenv = StreamTableEnvironment.create(senv);
        return makeMlEnv(benv, btenv, senv, stenv);
    }

    @Override
    public void destroy() {
        miniClusterResource.after();
    }
}
