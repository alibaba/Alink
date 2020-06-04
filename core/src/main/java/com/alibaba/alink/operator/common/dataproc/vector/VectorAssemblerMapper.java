package com.alibaba.alink.operator.common.dataproc.vector;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.MISOMapper;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.dataproc.vector.VectorAssemblerParams;
import com.alibaba.alink.params.shared.HasHandleInvalid.HandleInvalidMethod;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * This mapper maps many columns to one vector. the columns should be vector or numerical columns.
 */
public class VectorAssemblerMapper extends MISOMapper {
    /**
     * this variable using to judge the output vector format.
     * if nnz * ratio > vector size, the vector format is denseVector, else sparseVector.
     *
     * if number of values in the vector is less than 1/RATIO of its length, then use SparseVector.
     * else use DenseVector.
     */
    private static final double RATIO = 1.5;

    /**
     * the way to handle invalid input.
     */
    private HandleInvalidMethod handleInvalid;

    public VectorAssemblerMapper(TableSchema dataSchema, Params params) {
        super(dataSchema, params);
        this.handleInvalid = params.get(VectorAssemblerParams.HANDLE_INVALID);
    }

    @Override
    protected TypeInformation initOutputColType() {
        return VectorTypes.VECTOR;
    }

    /**
     * each column of the input may be a number or a Vector.
     */
    @Override
    protected Object map(Object[] input) {
        if (null == input) {
            return null;
        }
        int pos = 0;
        Map<Integer, Double> map = new HashMap<>();
        // getVector the data, and write it in List.
        for (Object col : input) {
            if (null != col) {
                if (col instanceof Number) {
                    map.put(pos++, ((Number)col).doubleValue());
                } else if (col instanceof String) {
                    Vector vec = VectorUtil.getVector(col);
                    pos = appendVector(vec, map, pos);
                } else if (col instanceof Vector) {
                    pos = appendVector((Vector) col, map, pos);
                } else {
                    throw new UnsupportedOperationException("not support type of object.");
                }
            } else {
                switch (handleInvalid) {
                    case ERROR:
                        throw new NullPointerException("null value is found in vector assembler inputs.");
                    case SKIP:
                        return null;
                    default:
                }
            }
        }

		/* form the vector, and finally toString it. */
        Vector vec = new SparseVector(pos, map);

        if (map.size() * RATIO > pos) {
            vec = ((SparseVector)vec).toDenseVector();
        }
        return vec;
    }

    private int appendVector(Vector vec, Map<Integer, Double> map, int pos)
    {
        if (vec instanceof SparseVector) {
            SparseVector sv = (SparseVector)vec;
            int[] idx = sv.getIndices();
            double[] values = sv.getValues();
            for (int j = 0; j < idx.length; ++j) {
                map.put(pos + idx[j], values[j]);
            }
            pos += sv.size();
        } else if (vec instanceof DenseVector) {
            DenseVector dv = (DenseVector)vec;
            for (int j = 0; j < dv.size(); ++j) {
                map.put(pos++, dv.get(j));
            }
        }
        return pos;
    }
}
