package com.alibaba.alink.common.model;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * A {@link ModelSource} implementation that reads the model from the memory.
 */
public class RowsModelSource implements ModelSource {

    /**
     * The rows that hosts the model.
     */
    private final List<Row> modelRows;

    public RowsModelSource(List<Row> modelRows) {
        this.modelRows = modelRows;
    }

    @Override
    public List<Row> getModelRows(RuntimeContext runtimeContext) {
        return modelRows;
    }
}
