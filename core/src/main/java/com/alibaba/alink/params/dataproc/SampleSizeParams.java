package com.alibaba.alink.params.dataproc;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.ParamValidator;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface SampleSizeParams<T> extends
        WithParams<T> {

    ParamInfo<String> SIZE = ParamInfoFactory
            .createParamInfo("size", String.class)
            .setDescription("sampling ratio, it should be int value when as a number , defintion format as name1:number1,name2:number2 when as a string")
            .setRequired()
            .setValidator(new RatioValidator())
            .build();

    default String getSize() {
        return getParams().get(SIZE);
    }

    default T setSize(String value) {
        return set(SIZE, value);
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
