package com.alibaba.alink.operator.batch.utils;

import java.util.function.BiFunction;

import com.alibaba.alink.common.mapper.FlatMapper;
import com.alibaba.alink.common.mapper.FlatMapperAdapter;
import com.alibaba.alink.operator.batch.BatchOperator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

/**
 * class for a flat map {@link BatchOperator}.
 *
 * @param <T> class type of the {@link FlatMapBatchOp} implementation itself.
 */
public class FlatMapBatchOp<T extends FlatMapBatchOp<T>> extends BatchOperator<T> {

    private final BiFunction<TableSchema, Params, FlatMapper> mapperBuilder;

    public FlatMapBatchOp(BiFunction<TableSchema, Params, FlatMapper> mapperBuilder, Params params) {
        super(params);
        this.mapperBuilder = mapperBuilder;
    }


    @Override
    public T linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);

        try {
            FlatMapper flatmapper = this.mapperBuilder.apply(in.getSchema(), this.getParams());
            DataSet<Row> resultRows = in.getDataSet().flatMap(new FlatMapperAdapter(flatmapper));
            TableSchema resultSchema = flatmapper.getOutputSchema();
            this.setOutput(resultRows, resultSchema);
            return (T) this;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

}
