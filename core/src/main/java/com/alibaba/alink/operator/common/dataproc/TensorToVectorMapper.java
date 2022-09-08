package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.NumericalTensor;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.params.dataproc.TensorToVectorParams;
import com.alibaba.alink.params.dataproc.TensorToVectorParams.ConvertMethod;

public class TensorToVectorMapper extends SISOMapper {

	private final ConvertMethod method;

	public TensorToVectorMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		method = params.get(TensorToVectorParams.CONVERT_METHOD);
	}

	@Override
	protected Object mapColumn(Object input) {
		if (null == input) {
			return null;
		}

		Tensor <?> tensor = TensorUtil.getTensor(input);

		if (!(tensor instanceof NumericalTensor)) {
			throw new IllegalStateException(
				String.format(
					"Only numerical tensor could be converted to vector. Tensor type: %s",
					tensor.getClass().getName()
				)
			);
		}

		switch (method) {
			case FLATTEN:
				return DoubleTensor.of(tensor.flatten(0, -1)).toVector();
			case SUM:
				return DoubleTensor.of(((NumericalTensor <?>) tensor).sum(0, false)).toVector();
			case MEAN:
				return DoubleTensor.of(((NumericalTensor <?>) tensor).mean(0, false)).toVector();
			case MAX:
				return DoubleTensor.of(((NumericalTensor <?>) tensor).max(0, false)).toVector();
			case MIN:
				return DoubleTensor.of(((NumericalTensor <?>) tensor).min(0, false)).toVector();
			default:
				throw new AkUnsupportedOperationException("Not support exception. ");
		}
	}

	@Override
	protected TypeInformation initOutputColType() {
		return AlinkTypes.DENSE_VECTOR;
	}
}
