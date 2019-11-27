package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.dataproc.vector.VectorNormalizeParams;

/**
 * This mapper maps a vector with a new vector which divided by norm-p.
 */
public class VectorNormalizeMapper extends SISOMapper {
    private double p;

    /**
     * constructor.
     *
     * @param dataSchema input data schema
     * @param params     parameters for this mapper
     */
    public VectorNormalizeMapper(TableSchema dataSchema, Params params) {
        super(dataSchema, params);
        this.p = this.params.get(VectorNormalizeParams.P);
    }

    /**
     * get type of processing object.
     * @return type of vector
     */
    @Override
    protected TypeInformation initOutputColType() {
        return VectorTypes.VECTOR;
    }

    /**
     *
     * @param input the input object
     * @return      predict result
     */
    @Override
    protected Object mapColumn(Object input) {
        if (null == input) {
            return null;
        }
        Vector vec = VectorUtil.getVector(input);
        vec.normalizeEqual(p);
        return vec;
    }
}
