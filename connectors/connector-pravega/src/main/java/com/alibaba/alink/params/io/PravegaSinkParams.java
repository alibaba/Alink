package com.alibaba.alink.params.io;

import com.alibaba.alink.params.io.HasFieldDelimiterDvComma;
import com.alibaba.alink.params.io.shared_params.HasDataFormat;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface PravegaSinkParams<T> extends WithParams<T>, HasDataFormat<T>, HasFieldDelimiterDvComma<T> {

    /**
     * Parameter of pravega controller uri
     **/
    ParamInfo<String> PRAVEGA_CONTROLLER_URI = ParamInfoFactory
            .createParamInfo("pravega_controller_uri", String.class)
            .setDescription("pravega controller uri")
            .setRequired()
            .setAlias(new String[]{"pravega.controllerUri", "pravega_controller_uri"})
            .build();
    /**
     * Parameter of pravega scope
     **/
    ParamInfo<String> PRAVEGA_SCOPE = ParamInfoFactory
            .createParamInfo("pravega_scope", String.class)
            .setDescription("pravega scope")
            .setRequired()
            .setAlias(new String[]{"pravega.scope", "pravega_scope"})
            .build();
    ParamInfo<String> PRAVEGA_STREAM = ParamInfoFactory
            .createParamInfo("pravega_stream", String.class)
            .setDescription("pravega stream")
            .setRequired()
            .setAlias(new String[]{"pravega.stream", "pravega_stream"})
            .build();
    /**
     * Parameter of pravega writer mode, only be used by the PravegaSinkStreamOp
     **/
    ParamInfo<String> PRAVEGA_WRITER_MODE = ParamInfoFactory
            .createParamInfo("pravega_writer_mode", String.class)
            .setDescription("pravega writer mode")
            .setOptional()
            .setHasDefaultValue("ATLEAST_ONCE")
            .setAlias(new String[]{"pravega.writerMode", "pravega_writer_mode"})
            .build();
    /**
     * Parameter of pravega standalone indicator
     **/
    ParamInfo<String> PRAVEGA_ROUTING_KEY = ParamInfoFactory
            .createParamInfo("pravega_routing_key", String.class)
            .setDescription("pravega routing key")
            .setOptional()
            .setHasDefaultValue("default")
            .setAlias(new String[]{"pravega.routingKey", "pravega_routing_key"})
            .build();
    /**
     * Parameter of pravega standalone indicator
     **/
    ParamInfo<Boolean> PRAVEGA_STANDALONE = ParamInfoFactory
            .createParamInfo("PRAVEGA_STANDALONE", Boolean.class)
            .setDescription("pravega standalone")
            .setOptional()
            .setHasDefaultValue(true)
            .setAlias(new String[]{"pravega.standalone", "pravega_standalone"})
            .build();

    default String getPravegaControllerUri() {
        return get(PRAVEGA_CONTROLLER_URI);
    }

    default T setPravegaControllerUri(String value) {
        return set(PRAVEGA_CONTROLLER_URI, value);
    }

    default String getPravegaScope() {
        return get(PRAVEGA_SCOPE);
    }

    /**
     * Parameter of pravega stream
     **/
    default T setPravegaScope(String value) {
        return set(PRAVEGA_SCOPE, value);
    }

    default String getPravegaStream() {
        return get(PRAVEGA_STREAM);
    }

    default T setPravegaStream(String value) {
        return set(PRAVEGA_STREAM, value);
    }

    default String getPravegaWriterMode() {
        return get(PRAVEGA_WRITER_MODE);
    }

    default T setPravegaPravegaWriterMode(String value) {
        return set(PRAVEGA_WRITER_MODE, value);
    }

    default String getPravegaRoutingKey() {
        return get(PRAVEGA_ROUTING_KEY);
    }

    default T setPravegaRoutingKey(String value) {
        return set(PRAVEGA_ROUTING_KEY, value);
    }

    default Boolean getPravegaStandalone() {
        return get(PRAVEGA_STANDALONE);
    }

    default T setPravegaStandalone(Boolean value) {
        return set(PRAVEGA_STANDALONE, value);
    }


}
