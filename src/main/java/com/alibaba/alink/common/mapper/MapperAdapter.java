package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Adapt a {@link Mapper} to run within flink.
 */
public class MapperAdapter implements MapFunction<Row, Row>, Serializable {

    private final Mapper mapper;

    public MapperAdapter(Mapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Row map(Row row) throws Exception {
        return this.mapper.map(row);
    }
}
