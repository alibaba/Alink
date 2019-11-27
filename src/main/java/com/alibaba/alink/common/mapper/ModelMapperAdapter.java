package com.alibaba.alink.common.mapper;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.model.ModelSource;

import java.io.Serializable;
import java.util.List;

/**
 * Adapt a {@link ModelMapper} to run within flink.
 * <p>
 * This adapter class hold the target {@link ModelMapper} and it's {@link ModelSource}. Upon open(),
 * it will load model rows from {@link ModelSource} into {@link ModelMapper}.
 */
public class ModelMapperAdapter extends RichMapFunction<Row, Row> implements Serializable {

    /**
     * The ModelMapper to adapt.
     */
    private final ModelMapper mapper;

    /**
     * Load model data from ModelSource when open().
     */
    private final ModelSource modelSource;

    public ModelMapperAdapter(ModelMapper mapper, ModelSource modelSource) {
        this.mapper = mapper;
        this.modelSource = modelSource;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        List<Row> modelRows = this.modelSource.getModelRows(getRuntimeContext());
        this.mapper.loadModel(modelRows);
    }

    @Override
    public Row map(Row row) throws Exception {
        return this.mapper.map(row);
    }
}
