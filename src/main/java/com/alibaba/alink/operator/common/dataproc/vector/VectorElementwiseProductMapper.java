package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.dataproc.vector.VectorElementwiseProductParams;

/**
 * This mapper maps a vector to a new vector with special scale.
 */
public class VectorElementwiseProductMapper extends SISOMapper {
	private Vector scalingVector;

	public VectorElementwiseProductMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.scalingVector = VectorUtil.getVector(this.params.get(VectorElementwiseProductParams.SCALING_VECTOR));
	}

	@Override
	protected TypeInformation initOutputColType() {
		return VectorTypes.VECTOR;
	}

	@Override
	protected Object mapColumn(Object input) {
		if (null == input) {
			return null;
		}

		Vector vector = VectorUtil.getVector(input);

		if (vector instanceof DenseVector) {
			double[] vec = ((DenseVector) vector).getData();
			for (int i = 0; i < vec.length; ++i) {
				vec[i] = vec[i] * scalingVector.get(i);
			}
		} else {
			SparseVector vec = (SparseVector) vector;
			double[] vecValues = vec.getValues();
			int[] vecIndices = vec.getIndices();

			for (int i = 0; i < vecValues.length; ++i) {
				vecValues[i] *= scalingVector.get(vecIndices[i]);
			}
		}
		return vector;
	}

}
