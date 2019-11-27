package com.alibaba.alink.common.model;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * A {@link ModelSource} implementation that reads the model from the broadcast variable.
 */
public class BroadcastVariableModelSource implements ModelSource {

    /**
     * The name of the broadcast variable that hosts the model.
     */
    private final String modelVariableName;

    public BroadcastVariableModelSource(String modelVariableName) {
        this.modelVariableName = modelVariableName;
    }

    @Override
    public List<Row> getModelRows(RuntimeContext runtimeContext) {
        return runtimeContext.getBroadcastVariable(modelVariableName);
    }
}