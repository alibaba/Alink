package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.linalg.MatVecOp;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.MISOMapper;
import com.alibaba.alink.params.dataproc.vector.VectorBiFunctionParams;
import com.alibaba.alink.params.shared.HasBiFuncName;

/**
 * Vector operator with two input vectors.
 */
public class VectorBiFunctionMapper extends MISOMapper {

	private static final long serialVersionUID = 4894734419116722829L;
	private final HasBiFuncName.BiFuncName funcName;

	public VectorBiFunctionMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.funcName = this.params.get(VectorBiFunctionParams.BI_FUNC_NAME);
	}

	@Override
	protected TypeInformation <?> initOutputColType() {
		switch (this.params.get(VectorBiFunctionParams.BI_FUNC_NAME)) {
			case Minus:
			case Plus:
			case ElementWiseMultiply:
			case Merge:
				return AlinkTypes.VECTOR;
			case Dot:
			case EuclidDistance:
			case Cosine:
				return Types.DOUBLE;
			default:
				throw new AkUnsupportedOperationException("not support yet.");
		}
	}

	@Override
	protected Object map(Object[] input) {
		Vector vec1 = VectorUtil.getVector(input[0]);
		Vector vec2 = VectorUtil.getVector(input[1]);
		switch (this.funcName) {
			case Minus:
				return MatVecOp.minus(vec1, vec2);
			case Plus:
				return MatVecOp.plus(vec1, vec2);
			case ElementWiseMultiply:
				return MatVecOp.elementWiseMultiply(vec1, vec2);
			case Merge:
				return MatVecOp.mergeVector(vec1, vec2);
			case Dot:
				return MatVecOp.dot(vec1, vec2);
			case EuclidDistance:
				return MatVecOp.euclidDistance(vec1, vec2);
			case Cosine:
				return MatVecOp.cosine(vec1, vec2);
			default:
				throw new AkUnsupportedOperationException("not support yet!");
		}
	}
}
