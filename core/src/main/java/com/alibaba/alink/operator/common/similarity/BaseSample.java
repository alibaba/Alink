package com.alibaba.alink.operator.common.similarity;

import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Base class to save the data for calculating distance fast.
 * @param <T> the type of the pre-computed info.
 */
public class BaseSample<T> implements Serializable {
    private Row row;

    private T label;

    private String str;

    public String getStr() {
        return str;
    }

    public BaseSample(String str, Row row, T label) {
        this.row = row;
        this.label = label;
        this.str = str;
    }

    public Row getRow() {
        return row;
    }

    public void setRow(Row row) {
        this.row = row;
    }

    public T getLabel() {
        return label;
    }

    public static String[] split(String str){
        return str.split(" ", -1);
    }
}
