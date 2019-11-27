package com.alibaba.alink.common.model;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.List;

/**
 * An interface that load the model from different sources. E.g. broadcast variables, list of rows, etc.
 */
public interface ModelSource extends Serializable {

    /**
     * Get the rows that containing the model.
     *
     * @return the rows that containing the model.
     */
    List<Row> getModelRows(RuntimeContext runtimeContext);
}
