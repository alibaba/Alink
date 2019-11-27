package com.alibaba.alink.common.utils;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;

/**
 * Utils for handling dataset.
 */
public class DataSetUtil {
    /**
     * Count number of records in the dataset.
     *
     * @return a dataset of one record, recording the number of records of [[dataset]]
     */
    public static <T> DataSet<Long> count(DataSet<T> dataSet) {
        return dataSet
            .mapPartition(new MapPartitionFunction<T, Long>() {
                @Override
                public void mapPartition(Iterable<T> values, Collector<Long> out) throws Exception {
                    long cnt = 0L;
                    for (T v : values) {
                        cnt++;
                    }
                    out.collect(cnt);
                }
            })
            .name("count_dataset")
            .returns(Types.LONG)
            .reduce(new ReduceFunction<Long>() {
                @Override
                public Long reduce(Long value1, Long value2) throws Exception {
                    return value1 + value2;
                }
            });
    }

    /**
     * Returns an empty dataset of the same type as [[dataSet]].
     */
    public static <T> DataSet<T> empty(DataSet<T> dataSet) {
        return dataSet
            .mapPartition(new MapPartitionFunction<T, T>() {
                @Override
                public void mapPartition(Iterable<T> values, Collector<T> out) throws Exception {
                }
            })
            .returns(dataSet.getType());
    }
}
