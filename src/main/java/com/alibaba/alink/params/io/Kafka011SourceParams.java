package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.WithParams;
import com.alibaba.alink.params.io.shared_params.HasStartTime_null;
import com.alibaba.alink.params.io.shared_params.HasTopicPattern_null;
import com.alibaba.alink.params.io.shared_params.HasTopic_null;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface Kafka011SourceParams<T> extends WithParams<T>,
	HasTopic_null <T>, HasTopicPattern_null <T>, HasStartTime_null <T> {

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

    ParamInfo <String> GROUP_ID = ParamInfoFactory
        .createParamInfo("groupId", String.class)
        .setDescription("消费组id")
        .setRequired()
        .setAlias(new String[]{"group.id"})
        .build();

    default String getGroupId() {
        return get(GROUP_ID);
    }

    default T setGroupId(String value) {
        return set(GROUP_ID, value);
    }

    ParamInfo <String> STARTUP_MODE = ParamInfoFactory
        .createParamInfo("startupMode", String.class)
        .setDescription("startupMode")
        .setRequired()
        .build();

    default String getStartupMode() {
        return get(STARTUP_MODE);
    }

    default T setStartupMode(String value) {
        return set(STARTUP_MODE, value);
    }
}
