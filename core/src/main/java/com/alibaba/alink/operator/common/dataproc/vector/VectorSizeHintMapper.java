package com.alibaba.alink.operator.common.dataproc.vector;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.params.dataproc.vector.VectorSizeHintParams;
import com.alibaba.alink.params.shared.HasSize;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

/**
 * This mapper checks the size of vector and give results as parameter define.
 */
public class VectorSizeHintMapper extends SISOMapper {
    private int size;

    private enum HandleType {
        /**
         * If the input vector is null or its size does not match the given one, then throw exception.
         */
        ERROR,
        /**
         * It will accept the vector if the vector is not null.
         */
        OPTIMISTIC
    }

    private HandleType handleMethod;

    public VectorSizeHintMapper(TableSchema dataSchema, Params params) {
        super(dataSchema, params);
        this.handleMethod = HandleType.valueOf(this.params.get(VectorSizeHintParams.HANDLE_INVALID).toUpperCase());
        this.size = this.params.get(HasSize.SIZE);
    }

    @Override
    protected TypeInformation initOutputColType() {
        return VectorTypes.VECTOR;
    }

    @Override
    protected Object mapColumn(Object input) throws Exception {
        Vector vec;
        switch (handleMethod) {
            case ERROR:
                if (input == null) {
                    throw new NullPointerException(
                        "Got null vector in VectorSizeHint");
                } else {
                    vec = VectorUtil.getVector(input);
                    if (vec.size() == size) {
                        return vec;
                    } else {
                        throw new IllegalArgumentException(
                            "VectorSizeHint : vec size (" + vec.size() + ") not equal param size (" + size + ").");
                    }
                }
            case OPTIMISTIC:
                if (input != null) {
                    return VectorUtil.getVector(input);
                } else {
                    return null;
                }
            default:
                throw new IllegalArgumentException("Not support param " + handleMethod);
        }
    }
}
