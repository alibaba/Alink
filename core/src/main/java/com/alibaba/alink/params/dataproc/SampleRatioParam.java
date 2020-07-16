package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.ParamValidator;
import org.apache.flink.ml.api.misc.param.WithParams;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface SampleRatioParam<T> extends
        WithParams<T>  {

    Pattern compile = Pattern.compile("(^0$)|(^0\\.\\d{1,2}$)|(^1$)");


    ParamInfo<String> RATIO = ParamInfoFactory
            .createParamInfo("ratio", String.class)
            .setDescription("sampling ratio, it should be in range of [0, 1] when as a number , defintion format as name1:number1,name2:number2 when as a string")
            .setRequired()
            .setValidator(new RatioValidator())
            .build();

    default String getRatio() {
        return getParams().get(RATIO);
    }

    default T setRatio(String value) {
        return set(RATIO, value);
    }
    final class RatioValidator implements ParamValidator<String> {

        @Override
        public boolean validate(String value) {
            Matcher matcher = compile.matcher(value);
            if (matcher.find()) {
                return true;
            }
            String[] splits = value.split(",");
            for (String string : splits) {
                String[] subSplits = string.split(":");
                if (subSplits.length != 2){
                    return false;
                }

                if (!compile.matcher(subSplits[1]).find()){
                    return false;
                }
            }

            return true;
        }

    }
}
