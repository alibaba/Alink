package com.alibaba.alink.params.io;

import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.io.shared.HasStartTimeDefaultAsNull;
import com.alibaba.alink.params.io.shared.HasTopicDefaultAsNull;
import com.alibaba.alink.params.io.shared.HasTopicPatternDefaultAsNull;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import java.io.Serializable;

public interface KafkaSourceParams<T> extends WithParams<T>,
    HasTopicDefaultAsNull <T>, HasTopicPatternDefaultAsNull <T>, HasStartTimeDefaultAsNull<T>, HasProperties<T> {

    ParamInfo<String> BOOTSTRAP_SERVERS = ParamInfoFactory
        .createParamInfo("bootstrapServers", String.class)
        .setDescription("kafka bootstrap servers")
        .setRequired()
        .setAlias(new String[]{"bootstrap.servers"})
        .build();

    default String getBootstrapServers() {
        return get(BOOTSTRAP_SERVERS);
    }

    default T setBootstrapServers(String value) {
        return set(BOOTSTRAP_SERVERS, value);
    }

    ParamInfo<String> GROUP_ID = ParamInfoFactory
        .createParamInfo("groupId", String.class)
        .setDescription("group id")
        .setRequired()
        .setAlias(new String[]{"group.id"})
        .build();

    default String getGroupId() {
        return get(GROUP_ID);
    }

    default T setGroupId(String value) {
        return set(GROUP_ID, value);
    }

    ParamInfo<StartupMode> STARTUP_MODE = ParamInfoFactory
        .createParamInfo("startupMode", StartupMode.class)
        .setDescription("startupMode")
        .setRequired()
        .build();

    default StartupMode getStartupMode() {
        return get(STARTUP_MODE);
    }

    default T setStartupMode(StartupMode value) {
        return set(STARTUP_MODE, value);
    }

    default T setStartupMode(String value) {
        return set(STARTUP_MODE, ParamUtil.searchEnum(STARTUP_MODE, value));
    }

    enum StartupMode implements Serializable {
        EARLIEST, GROUP_OFFSETS, LATEST, TIMESTAMP
    }
}
