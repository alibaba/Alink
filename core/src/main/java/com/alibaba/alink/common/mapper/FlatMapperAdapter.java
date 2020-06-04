package com.alibaba.alink.common.mapper;

import java.io.Serializable;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Adapt a {@link Mapper} to run within flink.
 */
public class FlatMapperAdapter extends RichFlatMapFunction<Row, Row> implements Serializable {

    private final FlatMapper mapper;

    public FlatMapperAdapter(FlatMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        mapper.open();
    }

    @Override
    public void close() throws Exception {
        mapper.close();
    }

    @Override
    public void flatMap(Row value, Collector<Row> out) throws Exception {
        this.mapper.flatMap(value, out);
    }
}
