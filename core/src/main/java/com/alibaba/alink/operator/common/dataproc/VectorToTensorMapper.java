package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.linalg.tensor.DataType;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.Shape;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.TensorTypes;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.params.dataproc.VectorToTensorParams;
import org.apache.commons.lang3.ArrayUtils;

public class VectorToTensorMapper extends SISOMapper {
	private final long[] shape;
	private final DataType targetDataType;

	public VectorToTensorMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);

		Long[] shapeObj = params.get(VectorToTensorParams.TENSOR_SHAPE);

		shape = shapeObj == null ? null : ArrayUtils.toPrimitive(shapeObj);

		targetDataType = getTargetDataType(params);
	}

	@Override
	protected Object mapColumn(Object input) {
		if (null == input) {
			return null;
		}

		Tensor <?> tensor = TensorUtil.getTensor(input);

		if (DataType.DOUBLE.equals(targetDataType)) {
			tensor = DoubleTensor.of(tensor);
		} else if (DataType.FLOAT.equals(targetDataType)) {
			tensor = FloatTensor.of(tensor);
		}

		if (shape == null) {
			return tensor;
		}

		return tensor.reshape(new Shape(shape));
	}

	@Override
	protected TypeInformation initOutputColType() {
		DataType targetDataType = getTargetDataType(params);

		if (DataType.DOUBLE.equals(targetDataType)) {
			return TensorTypes.DOUBLE_TENSOR;
		}

		if (DataType.FLOAT.equals(targetDataType)) {
			return TensorTypes.FLOAT_TENSOR;
		}

		return TensorTypes.DOUBLE_TENSOR;
	}

	private static DataType getTargetDataType(Params params) {
		String dataTypeStr = params.get(VectorToTensorParams.TENSOR_DATA_TYPE);

		if (dataTypeStr == null) {
			return DataType.DOUBLE;
		}

		dataTypeStr = dataTypeStr.trim().toUpperCase();

		if (dataTypeStr.isEmpty()) {
			return DataType.DOUBLE;
		}
		if (dataTypeStr.equals("DOUBLE")) {
			return DataType.DOUBLE;
		}
		if (dataTypeStr.equals("FLOAT")) {
			return DataType.FLOAT;
		}

		throw new UnsupportedOperationException("Unsupported tensor data type: " + dataTypeStr);
	}
}
