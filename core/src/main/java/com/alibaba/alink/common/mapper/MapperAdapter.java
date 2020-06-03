package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Adapt a {@link Mapper} to run within flink.
 */
public class MapperAdapter extends RichMapFunction<Row, Row> implements Serializable {

    private final Mapper mapper;

    public MapperAdapter(Mapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Row map(Row row) throws Exception {
        return this.mapper.map(row);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.mapper.open();
    }

    @Override
    public void close() throws Exception {
        this.mapper.close();
    }
}
