package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface SampleSeedParam<T> extends
        WithParams<T>  {

    ParamInfo<Long> SEED = ParamInfoFactory
            .createParamInfo("seed", Long.class)
            .setDescription("seed of random")
            .build();

    default Long getSeed() {
        return getParams().get(SEED);
    }

    default T setSeed(long value) {
        return set(SEED, value);
    }
}
