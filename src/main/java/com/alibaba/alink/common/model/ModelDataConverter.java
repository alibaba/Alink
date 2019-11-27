package com.alibaba.alink.common.model;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;


/**
 * This is the interface for saving/loading model data to/from a collection of {@link Row}s.
 *
 * @param <M1> Type of model data that would be saved to a collection of rows.
 * @param <M2> Type of model data that is loaded from a collection of rows.
 */
public interface ModelDataConverter<M1, M2> {
    /**
     * Save the model data to a collection of rows.
     *
     * @param modelData The model data to save.
     * @param collector A collector for rows.
     */
    void save(M1 modelData, Collector<Row> collector);

    /**
     * Load the model data from a collection of rows.
     *
     * @param rows The collection of rows from which model data is loaded.
     * @return The model data.
     */
    M2 load(List<Row> rows);

    /**
     * Get the schema of the table to which the model data is saved.
     *
     * @return The schema.
     */
    TableSchema getModelSchema();
}
