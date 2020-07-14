package com.alibaba.alink.params.dataproc;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.ParamValidator;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface StratifiedSampleWithSizeParams<T> extends
        WithParams<T> {


    ParamInfo<String> GROUP_KEY = ParamInfoFactory
            .createParamInfo("groupKey", String.class)
            .setDescription("name of group key")
            .setRequired()
            .build();

    ParamInfo<String> SIZE = ParamInfoFactory
            .createParamInfo("size", String.class)
            .setDescription("sampling ratio, it should be int value when as a number , defintion format as name1:number1,name2:number2 when as a string")
            .setRequired()
            .setValidator(new RatioValidator())
            .build();

    ParamInfo<Long> SEED = ParamInfoFactory
            .createParamInfo("seed", Long.class)
            .setDescription("seed of random")
            .setOptional()
            .build();

    ParamInfo<Boolean> WITH_REPLACEMENT = ParamInfoFactory
            .createParamInfo("withReplacement", Boolean.class)
            .setDescription("Indicates whether to enable sampling with replacement, default is without replcement")
            .setHasDefaultValue(false)
            .build();


    default String getGroupKey() {
        return getParams().get(GROUP_KEY);
    }

    default T setGroupKey(String value) {
        return set(GROUP_KEY, value);
    }

    default String getSize() {
        return getParams().get(SIZE);
    }

    default T setSize(String value) {
        return set(SIZE, value);
    }

    default Long getSeed() {
        return getParams().get(SEED);
    }

    default T setSeed(Long value) {
        return set(SEED, value);
    }

    default Boolean getWithReplacement() {
        return getParams().get(WITH_REPLACEMENT);
    }

    default T setWithReplacement(Boolean value) {
        return set(WITH_REPLACEMENT, value);
    }



    final class RatioValidator implements ParamValidator<String> {

        @Override
        public boolean validate(String value) {
            if (StringUtils.isNumeric(value)) {
                return true;
            }
            String[] array = value.split(",");
            for (String string : array) {
                String[] kv = string.split(":");
                if (kv.length != 2){
                    return false;
                }
                if (!StringUtils.isNumeric(value)){
                    return false;
                }
            }

            return true;
        }

    }




}
