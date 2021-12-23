package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.params.timeseries.ExpandExtendedVarsParams;

public class ExpandExtendedVarsMapper extends Mapper {
	private final int numVars;

	public ExpandExtendedVarsMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);

		numVars = params.get(ExpandExtendedVarsParams.NUM_VARS);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		final DenseVector vector = VectorUtil.getDenseVector(
			VectorUtil.getVector(selection.get(0))
		);

		final DenseVector cVector = VectorUtil.getDenseVector(
			VectorUtil.getVector(selection.get(1))
		);

		if (vector == null) {
			result.set(0, null);
			return;
		}

		if (cVector == null) {
			result.set(0, vector);
			return;
		}

		final int size = vector.size();
		final int cSize = cVector.size();

		final int gSize = size / numVars;
		final int cPlusSize = cSize + numVars;
		final int finalSize = gSize * cPlusSize;

		final double[] array = vector.getData();
		final double[] cArray = cVector.getData();

		final double[] buf = new double[finalSize];

		for (int i = 0; i < gSize; ++i) {
			final int s = i * cPlusSize;

			for (int j = i * numVars, j1 = s; j < (i + 1) * numVars; ++j, ++j1) {
				buf[j1] = array[j];
			}

			for (int j = 0, j1 = s + numVars; j < cSize; ++j, ++j1) {
				buf[j1] = cArray[j];
			}
		}

		result.set(0, new DenseVector(buf));
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params) {
		return Tuple4.of(
			new String[] {
				params.get(ExpandExtendedVarsParams.VECTOR_COL),
				params.get(ExpandExtendedVarsParams.EXTENDED_VECTOR_COL)
			},
			new String[] {
				params.get(ExpandExtendedVarsParams.OUTPUT_COL)
			},
			new TypeInformation <?>[] {
				VectorTypes.DENSE_VECTOR
			},
			params.get(ExpandExtendedVarsParams.RESERVED_COLS)
		);
	}
}
