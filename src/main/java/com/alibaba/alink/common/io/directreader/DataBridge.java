package com.alibaba.alink.common.io.directreader;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.List;

/**
 * An DataBridge is used to connect a batch job and a stream job, usually the case in which
 * a stream job load model data produced by an batch job.
 */
public interface DataBridge extends Serializable {

    /**
     * Read data with filter.
     *
     * @param filter return true retain a row. When filter is null all data is retained.
     * @return result rows.
     */
    List<Row> read(FilterFunction<Row> filter);
}
