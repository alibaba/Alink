package com.alibaba.alink.operator.stream.utils;

import java.util.function.BiFunction;

import com.alibaba.alink.common.mapper.FlatMapper;
import com.alibaba.alink.common.mapper.FlatMapperAdapter;
import com.alibaba.alink.operator.stream.StreamOperator;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

/**
 * class for a flat map {@link StreamOperator}.
 *
 * @param <T> class type of the {@link FlatMapStreamOp} implementation itself.
 */
public class FlatMapStreamOp<T extends FlatMapStreamOp<T>> extends StreamOperator<T> {

    private final BiFunction<TableSchema, Params, FlatMapper> mapperBuilder;

    public FlatMapStreamOp(BiFunction<TableSchema, Params, FlatMapper> mapperBuilder, Params params) {
        super(params);
        this.mapperBuilder = mapperBuilder;
    }

    @Override
    public T linkFrom(StreamOperator<?>... inputs) {
        StreamOperator<?> in = checkAndGetFirst(inputs);

        try {
            FlatMapper flatMapper = this.mapperBuilder.apply(in.getSchema(), this.getParams());
            DataStream<Row> resultRows = in.getDataStream().flatMap(new FlatMapperAdapter(flatMapper));
            this.setOutput(resultRows, flatMapper.getOutputSchema());

            return (T) this;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
