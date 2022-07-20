package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorIterator;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.feature.BinarizerParams;

import java.lang.reflect.Constructor;
import java.util.Arrays;

/**
 * Binarize a continuous variable using a threshold. The features greater than threshold, will be binarized 1.0 and the
 * features equal to or less than threshold, will be binarized to 0.
 *
 * <p>Support Vector input and Number input.
 */
public class BinarizerMapper extends SISOMapper {
	private static final long serialVersionUID = 3404239364851551683L;
	private final double threshold;
	private final TypeInformation selectedColType;
	private Object objectValue0, objectValue1;

	public BinarizerMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.threshold = this.params.get(BinarizerParams.THRESHOLD);

		selectedColType = TableUtil.findColTypeWithAssertAndHint(
			dataSchema,
			this.params.get(BinarizerParams.SELECTED_COL)
		);

		if (TableUtil.isSupportedNumericType(selectedColType)) {
			try {
				Constructor constructor = selectedColType.getTypeClass().getConstructor(String.class);
				objectValue0 = constructor.newInstance("0");
				objectValue1 = constructor.newInstance("1");
			} catch (Exception e) {
				throw new AkIllegalOperatorParameterException("cannot create binary instance the same class as selectedColType");
			}
		}
	}

	@Override
	protected TypeInformation initOutputColType() {
		final TypeInformation <?> selectedColType = TableUtil.findColTypeWithAssertAndHint(
			getDataSchema(),
			this.params.get(BinarizerParams.SELECTED_COL)
		);

		if (TableUtil.isSupportedNumericType(selectedColType)) {
			return selectedColType;
		}

		return AlinkTypes.VECTOR;
	}

	/**
	 * If input is a vector, in the case of dense vector, all the features are compared with threshold, and return a
	 * vector in either dense or sparse format, whichever uses less storage. If input is a sparseVector, only compare
	 * those non-zero features, and returns a sparse vector.
	 *
	 * @param input data input, support number or Vector.
	 * @return If input is a number, compare the number with threshold.
	 * @throws IllegalArgumentException input is neither number nor vector.
	 */
	@Override
	protected Object mapColumn(Object input) throws Exception {
		if (null == input) {
			return null;
		}
		if (TableUtil.isSupportedNumericType(selectedColType)) {
			return ((Number) input).doubleValue() > threshold ? objectValue1 : objectValue0;
		} else if (TableUtil.isVector(selectedColType)) {
			Vector parseVector = VectorUtil.getVector(input);
			if (parseVector instanceof SparseVector) {
				SparseVector vec = (SparseVector) parseVector;
				VectorIterator vectorIterator = vec.iterator();
				int[] newIndices = new int[vec.numberOfValues()];
				int pos = 0;
				while (vectorIterator.hasNext()) {
					if (vectorIterator.getValue() > threshold) {
						newIndices[pos++] = vectorIterator.getIndex();
					}
					vectorIterator.next();
				}
				double[] newValues = new double[pos];
				Arrays.fill(newValues, 1.0);
				return new SparseVector(vec.size(), Arrays.copyOf(newIndices, pos), newValues);
			} else {
				DenseVector vec = (DenseVector) parseVector;
				double[] data = vec.getData();
				int[] newIndices = new int[vec.size()];
				int pos = 0;
				for (int i = 0; i < vec.size(); i++) {
					if (data[i] > threshold) {
						newIndices[pos++] = i;
						data[i] = 1.0;
					} else {
						data[i] = 0.0;
					}
				}
				return vec;
			}
		} else {
			throw new AkIllegalOperatorParameterException("Only support Number and vector!");
		}
	}
}
