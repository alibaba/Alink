package com.alibaba.alink.operator.common.distance;

import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;

/**
 * Save the data for calculating distance fast. The FastDistanceMatrixData
 */
public class FastDistanceVectorData extends FastDistanceData{
    /**
     * Stores the vector(sparse or dense).
     */
    final Vector vector;

    /**
     * Stores some extra info extracted from the vector. For example, if we want to save the L1 norm and L2 norm of the
     * vector, then the two values are viewed as a two-dimension label vector.
     */
    DenseVector label;

    /**
     * Constructor, initialize the vector data.
     * @param vec vector.
     */
    public FastDistanceVectorData(Vector vec){
        this(vec, null);
    }

    /**
     * Constructor, initialize the vector data and extra info.
     * @param vec vector.
     * @param row extra info besides the vector.
     */
    public FastDistanceVectorData(Vector vec, Row row){
        super(null == row ? null : new Row[]{row});
        Preconditions.checkNotNull(vec, "Vector should not be null!");
        this.vector = vec;
    }

    public FastDistanceVectorData(FastDistanceVectorData vectorData){
        super(vectorData);
        if(vectorData.vector instanceof SparseVector){
            this.vector = ((SparseVector)vectorData.vector).clone();
        } else {
            this.vector = ((DenseVector)vectorData.vector).clone();
        }
        this.label = (null == vectorData.label ? null : vectorData.label.clone());
    }


    public Vector getVector() {
        return vector;
    }

    public DenseVector getLabel() {
        return label;
    }
}
