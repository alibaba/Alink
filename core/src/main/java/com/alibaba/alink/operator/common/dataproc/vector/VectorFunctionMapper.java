package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.exceptions.AkUnimplementedOperationException;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.params.dataproc.vector.VectorFunctionParams;
import com.alibaba.alink.params.shared.HasFuncName;
import com.alibaba.alink.params.shared.HasFuncName.FuncName;

/**
 * Find maxValue / minValue / maxValue index / minValue index in Vector
 */

public class VectorFunctionMapper extends SISOMapper {

	private static final long serialVersionUID = 4894732419116722829L;
	private final HasFuncName.FuncName funcName;
	private double doubleVariable;

	public VectorFunctionMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.funcName = this.params.get(VectorFunctionParams.FUNC_NAME);
		switch (this.funcName) {
			case Scale:
			case Normalize:
				try {
					doubleVariable = Double.parseDouble(this.params.get(VectorFunctionParams.WITH_VARIABLE));
				} catch (Exception ex) {
					throw new IllegalArgumentException("Scale function need double type variable.");
				}
			default:
		}
	}

	@Override
	protected TypeInformation <?> initOutputColType() {
		switch (this.params.get(VectorFunctionParams.FUNC_NAME)) {
			case NormL1:
			case NormL2:
			case NormL2Square:
				return Types.DOUBLE;
			case Normalize:
			case Scale:
				return AlinkTypes.VECTOR;
			default:
				return Types.STRING;
		}
	}

	@Override
	protected Object mapColumn(Object input) {
		switch (this.funcName) {
			case Scale:
				return VectorUtil.getVector(input).scale(doubleVariable);
			case NormL2:
				return VectorUtil.getVector(input).normL2();
			case NormL1:
				return VectorUtil.getVector(input).normL1();
			case Normalize:
				Vector vec = (input instanceof Vector) ? ((Vector) input).clone() : VectorUtil.getVector(input);
				vec.normalizeEqual(doubleVariable);
				return vec;
			case NormL2Square:
				return VectorUtil.getVector(input).normL2Square();
			default:
				return procMaxMin(input);
		}
	}

	protected Object procMaxMin(Object input) {
		int dstIdx = 0;
		double dstVal;
		Vector vectorInput = VectorUtil.getVector(input);
		if (vectorInput == null || vectorInput.size() == 0) {
			return vectorInput;
		}
		boolean equalMin = funcName.equals(HasFuncName.FuncName.ArgMin) || funcName.equals(HasFuncName.FuncName.Min);
		boolean equalMax = funcName.equals(HasFuncName.FuncName.ArgMax) || funcName.equals(HasFuncName.FuncName.Max);
		if (vectorInput instanceof SparseVector) {
			SparseVector sv = (SparseVector) vectorInput;
			int[] indices = sv.getIndices();
			double[] values = sv.getValues();
			if (equalMax) {
				dstVal = Double.NEGATIVE_INFINITY;
				for (int i = 0; i < sv.numberOfValues(); ++i) {
					if (dstVal < values[i]) {
						dstVal = values[i];
						dstIdx = indices[i];
					}
				}
			} else if (equalMin) {
				dstVal = Double.POSITIVE_INFINITY;
				for (int i = 0; i < sv.numberOfValues(); ++i) {
					if (dstVal > values[i]) {
						dstVal = values[i];
						dstIdx = indices[i];
					}
				}
			} else {
				throw new AkUnimplementedOperationException("Not implemented yet!");
			}
		} else {
			DenseVector dv = (DenseVector) vectorInput;
			if (equalMax) {
				dstVal = Double.NEGATIVE_INFINITY;
				for (int i = 0; i < dv.size(); ++i) {
					if (dstVal < dv.get(i)) {
						dstVal = dv.get(i);
						dstIdx = i;
					}
				}
			} else if (equalMin) {
				dstVal = Double.POSITIVE_INFINITY;
				for (int i = 0; i < dv.size(); ++i) {
					if (dstVal > dv.get(i)) {
						dstVal = dv.get(i);
						dstIdx = i;
					}
				}
			} else {
				throw new AkUnimplementedOperationException("Not implemented yet!");
			}
		}

		if (funcName.equals(FuncName.ArgMax) || funcName.equals(FuncName.ArgMin)) {
			return String.valueOf(dstIdx);
		} else {
			return String.valueOf(dstVal);
		}
	}
}
