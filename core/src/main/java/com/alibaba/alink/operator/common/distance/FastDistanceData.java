package com.alibaba.alink.operator.common.distance;

import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Base class to save the data for calculating distance fast. It has two derived classes: FastDistanceVectorData and
 * FastDistanceMatrixData. FastDistanceVectorData saves only one vector(dense or spase) and FastDistanceMatrixData merges several
 * dense vectors in a matrix.
 */
public abstract class FastDistanceData implements Serializable, Cloneable {
    /**
     * Save the extra info besides the vector. Each vector is related to one row. Thus, for FastDistanceVectorData, the
     * length of <code>rows</code> is one. And for FastDistanceMatrixData, the length of <code>rows</code> is equal to
     * the number of cols of <code>matrix</code>. Besides, the order of the rows are the same with the vectors.
     */
    final Row[] rows;

    public Row[] getRows() {
        return rows;
    }

    FastDistanceData(Row[] rows){
        this.rows = rows;
    }

    FastDistanceData(FastDistanceData fastDistanceData){
        this.rows = null == fastDistanceData.rows ? null : fastDistanceData.rows.clone();
    }
}
