package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.WithParams;
import com.alibaba.alink.params.io.shared_params.HasDataFormat;
import com.alibaba.alink.params.io.shared_params.HasTopic;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface Kafka011SinkParams<T> extends WithParams<T>,
    HasTopic <T>, HasDataFormat <T>, HasFieldDelimiterDvComma <T> {

    ParamInfo <String> BOOTSTRAP_SERVERS = ParamInfoFactory
        .createParamInfo("bootstrapServers", String.class)
        .setDescription("kafka集群地址")
        .setRequired()
        .setAlias(new String[]{"bootstrap.servers", "boostrapServers"})
        .build();

    default String getBootstrapServers() {
        return get(BOOTSTRAP_SERVERS);
    }

    default T setBootstrapServers(String value) {
        return set(BOOTSTRAP_SERVERS, value);
    }
}
