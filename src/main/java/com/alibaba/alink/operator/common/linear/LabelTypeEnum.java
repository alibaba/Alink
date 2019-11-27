package com.alibaba.alink.operator.common.linear;

import com.alibaba.alink.common.utils.Functional;
import com.alibaba.alink.common.utils.JsonConverter;

import java.lang.reflect.Type;


public class LabelTypeEnum {

    /**
     * This enum helps recover label type from string.
     */
    public enum StringTypeEnum {

        INT(x -> JsonConverter.fromJson(x, (Type) Integer.class)),
        INTEGER(x -> JsonConverter.fromJson(x, (Type) Integer.class)),
        LONG(x -> JsonConverter.fromJson(x, (Type) Long.class)),
        BIGINT(x -> JsonConverter.fromJson(x, (Type) Long.class)),
        DOUBLE(x -> JsonConverter.fromJson(x, (Type) Double.class)),
        FLOAT(x -> JsonConverter.fromJson(x, (Type) Float.class)),
        BOOLEAN(x -> JsonConverter.fromJson(x, (Type) Boolean.class)),
        CHARACTER(x -> JsonConverter.fromJson(x, (Type) Character.class)),
        CHAR(x -> JsonConverter.fromJson(x, (Type) Character.class)),
        SHORT(x -> JsonConverter.fromJson(x, (Type) Short.class)),
        BYTE(x -> JsonConverter.fromJson(x, (Type) Byte.class)),
        STRING(x -> x);

        private Functional.SerializableFunction<String, Object> operation;

        StringTypeEnum(Functional.SerializableFunction<String, Object> operation) {
            this.operation = operation;
        }

        Functional.SerializableFunction<String, Object> getOperation() {
            return operation;
        }
    }

    /**
     * This enum helps recover label type from double.
     */
    enum DoubleTypeEnum {

        LONG(Double::longValue),
        BIGINT(Double::longValue),
        INT(Double::intValue),
        FLOAT(Double::floatValue),
        INTEGER(Double::intValue),
        DOUBLE(x -> x);

        private Functional.SerializableFunction<Double, Number> operation;

        DoubleTypeEnum(Functional.SerializableFunction<Double, Number> operation) {
            this.operation = operation;
        }
        Functional.SerializableFunction<Double, Number> getOperation() {
            return operation;
        }

    }
}
