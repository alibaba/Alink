package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

public interface PravegaSourceParams<T> extends WithParams<T> {

    ParamInfo<String> PRAVEGA_CONTROLLER_URI = ParamInfoFactory
            .createParamInfo("pravega_controller_uri", String.class)
            .setDescription("pravega controller uri")
            .setRequired()
            .setAlias(new String[]{"pravega.controller.uri", "pravega_controller_uri"})
            .build();
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
    ParamInfo<DeserializationSchema> pravega_deserializer = ParamInfoFactory
            .createParamInfo("pravega_deserializer ", DeserializationSchema.class)
            .setDescription("pravega deserializer")
            .setRequired()
            .setAlias(new String[]{"pravega.deserializer", "pravega_deserializer"})
            .build();
    ParamInfo<String> pravega_startStreamCut = ParamInfoFactory
            .createParamInfo("pravega_startStreamCut ", String.class)
            .setDescription("pravega startStreamCut")
            .setHasDefaultValue("UNBOUNDED")
            .setOptional()
            .setAlias(new String[]{"pravega.startStreamCut", "pravega_startStreamCut"})
            .build();
    ParamInfo<String> pravega_endStreamCut = ParamInfoFactory
            .createParamInfo("pravega_endtreamCut ", String.class)
            .setDescription("pravega endStreamCut")
            .setHasDefaultValue("UNBOUNDED")
            .setOptional()
            .setAlias(new String[]{"pravega.endStreamCut", "pravega_endtreamCut"})
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

    default T setPravegaScope(String value) {
        return set(PRAVEGA_SCOPE, value);
    }

    default String getPravegaStream() {
        return get(PRAVEGA_STREAM);
    }

    default T setPravegaStream(String value) {
        return set(PRAVEGA_STREAM, value);
    }

    default DeserializationSchema getPravegaDeserializer() {
        return get(pravega_deserializer);
    }

    default T setPravegaDeserializer(DeserializationSchema value) {
        return set(pravega_deserializer, value);
    }

    default String getPravegaStartStreamCut() {
        return get(pravega_startStreamCut);
    }

    default T setPravegaStartStreamCut(String value) {
        return set(pravega_startStreamCut, value);
    }

    default String getPravegaEndStreamCut() {
        return get(pravega_endStreamCut);
    }

    default T setPravegaEndStreamCut(String value) {
        return set(pravega_endStreamCut, value);
    }

}