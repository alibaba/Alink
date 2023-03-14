package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.MISOMapper;

/**
 * This mapper maps two vectors to one with interact operation.
 */
public class VectorInteractionMapper extends MISOMapper {

	private static final long serialVersionUID = 5122592154123233560L;

	public VectorInteractionMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	@Override
	protected TypeInformation<?> initOutputColType() {
		return AlinkTypes.VECTOR;
	}

	@Override
	protected Object map(Object[] input) {
		if (input.length != 2) {
			throw new AkIllegalDataException("VectorInteraction only support two input columns.");
		}

		if (input[0] == null || input[1] == null) {
			return null;
		}

		Vector vector1 = VectorUtil.getVector(input[0]);
		Vector vector2 = VectorUtil.getVector(input[1]);

		if (vector1 instanceof SparseVector) {
			if (vector2 instanceof DenseVector) {
				throw new AkIllegalDataException("Make sure the two input vectors are both dense or sparse.");
			}
			SparseVector sparseVector = (SparseVector) vector1;
			int vecSize = sparseVector.size();
			int[] indices = sparseVector.getIndices();
			double[] values = sparseVector.getValues();
			SparseVector scalingVector = (SparseVector) vector2;
			int scalingSize = scalingVector.size();
			int[] scalingIndices = scalingVector.getIndices();
			double[] scalingValues = scalingVector.getValues();
			double[] interactionValues = new double[scalingIndices.length * indices.length];
			int[] interactionIndices = new int[scalingIndices.length * indices.length];
			for (int i = 0; i < indices.length; ++i) {
				int idxBase = i * scalingIndices.length;
				for (int j = 0; j < scalingIndices.length; ++j) {
					int idx = idxBase + j;
					interactionIndices[idx] = vecSize * scalingIndices[j] + indices[i];
					interactionValues[idx] = values[i] * scalingValues[j];
				}
			}
			return new SparseVector(vecSize * scalingSize, interactionIndices, interactionValues);
		} else {
			if (vector2 instanceof SparseVector) {
				throw new AkIllegalDataException("Make sure the two input vectors are both dense or sparse.");
			}
			double[] vecArray = ((DenseVector) vector1).getData();
			double[] scalingArray = ((DenseVector) vector2).getData();
			DenseVector inter = new DenseVector(vecArray.length * scalingArray.length);
			double[] interArray = inter.getData();
			for (int i = 0; i < vecArray.length; ++i) {
				int idxBase = i * scalingArray.length;
				for (int j = 0; j < scalingArray.length; ++j) {
					interArray[idxBase + j] = vecArray[i] * scalingArray[j];
				}
			}
			return inter;
		}

	}
}
