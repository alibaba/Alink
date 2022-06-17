package com.alibaba.alink.testutil.envfactory.impl;

import java.util.Properties;

import com.alibaba.alink.testutil.envfactory.EnvTypeAnnotation;
import com.alibaba.alink.testutil.envfactory.BaseEnvFactory;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.api.java.RemoteEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

@EnvTypeAnnotation(name = "remote")
public class RemoteEnvFactoryImpl implements BaseEnvFactory {
    private ExecutionEnvironmentFactory executionEnvironmentFactory;
    private StreamExecutionEnvironmentFactory streamExecutionEnvironmentFactory;

    @Override
    public void initialize(Properties properties) {
        final String host = properties.getProperty("host");
        final int port = Integer.parseInt(properties.getProperty("port"));
        final int parallelism = Integer.parseInt(properties.getProperty("parallelism", "2"));
        final Configuration configuration = createConfiguration(properties);

        executionEnvironmentFactory = () -> {
            RemoteEnvironment remoteEnvironment = new RemoteEnvironment(host, port, configuration, null);
            remoteEnvironment.setParallelism(parallelism);
            return remoteEnvironment;
        };
        streamExecutionEnvironmentFactory = () -> {
            RemoteStreamEnvironment remoteStreamEnvironment = new RemoteStreamEnvironment(host, port, configuration);
            remoteStreamEnvironment.setParallelism(parallelism);
            return remoteStreamEnvironment;
        };
    }

    @Override
    public Object getMlEnv() {
        ExecutionEnvironment benv = executionEnvironmentFactory.createExecutionEnvironment();
        StreamExecutionEnvironment senv = streamExecutionEnvironmentFactory.createExecutionEnvironment();
        BatchTableEnvironment btenv = BatchTableEnvironment.create(benv);
        StreamTableEnvironment stenv = StreamTableEnvironment.create(senv);
        return makeMlEnv(benv, btenv, senv, stenv);
    }
}
