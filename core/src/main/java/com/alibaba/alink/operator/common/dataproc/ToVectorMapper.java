package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorType;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.params.dataproc.ToVectorParams;
import com.alibaba.alink.params.shared.HasHandleInvalid.HandleInvalidMethod;

public class ToVectorMapper extends SISOMapper {
	private final HandleInvalidMethod handleInvalidMethod;
	private final VectorType vectorType;
	public ToVectorMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		vectorType = params.get(ToVectorParams.VECTOR_TYPE);
		handleInvalidMethod = params.get(ToVectorParams.HANDLE_INVALID);
	}

	@Override
	protected Object mapColumn(Object input) {
		if (null == input) {
			return null;
		}
		Vector vec = null;
		try {
			vec = VectorUtil.getVector(input);
		} catch (Exception ex) {
			switch (handleInvalidMethod) {
				case ERROR:
					throw ex;
				case SKIP:
					break;
				default:
					throw new UnsupportedOperationException();
			}
		}
		if (vec == null) {
			return vec;
		}
		if (vectorType == null) {
			return vec;
		} else if (vectorType.equals(VectorType.DENSE)){
			if (vec instanceof DenseVector) {
				return vec;
			} else {
				return ((SparseVector)vec).toDenseVector();
			}
		} else {
			if (vec instanceof SparseVector) {
				return vec;
			} else {
				return ((DenseVector) vec).toSparseVector();
			}
		}
	}

	@Override
	protected TypeInformation <?> initOutputColType() {
		if (params.contains(ToVectorParams.VECTOR_TYPE)) {
			if (params.get(ToVectorParams.VECTOR_TYPE).equals(VectorType.DENSE)) {
				return VectorTypes.DENSE_VECTOR;
			} else {
				return VectorTypes.SPARSE_VECTOR;
			}
		} else {
			return VectorTypes.VECTOR;
		}
	}
}
