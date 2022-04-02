package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.linalg.tensor.DataType;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.Shape;
import com.alibaba.alink.common.linalg.tensor.StringTensor;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.params.dataproc.ToTensorParams;
import com.alibaba.alink.params.shared.HasHandleInvalid.HandleInvalidMethod;
import org.apache.commons.lang3.ArrayUtils;

public class ToTensorMapper extends SISOMapper {
	private final long[] shape;
	private final DataType targetDataType;
	private final HandleInvalidMethod handleInvalidMethod;

	public ToTensorMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);

		Long[] shapeObj = params.get(ToTensorParams.TENSOR_SHAPE);

		shape = shapeObj == null ? null : ArrayUtils.toPrimitive(shapeObj);

		if (params.contains(ToTensorParams.TENSOR_DATA_TYPE)) {
			targetDataType = params.get(ToTensorParams.TENSOR_DATA_TYPE);
		} else {
			targetDataType = null;
		}

		handleInvalidMethod = params.get(ToTensorParams.HANDLE_INVALID);
	}

	@Override
	protected Object mapColumn(Object input) {
		if (null == input) {
			return null;
		}

		Tensor <?> tensor = null;

		// pass tensor.
		if (DataType.STRING.equals(targetDataType) && input instanceof String) {
			tensor = new StringTensor((String) input);
		} else {
			try {
				tensor = TensorUtil.getTensor(input);
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
		}

		if (tensor == null) {
			return null;
		}

		// If the target data type is null, do not convert the tensor to target type.
		if (targetDataType == null) {

			if (shape == null) {
				return tensor;
			}

			return tensor.reshape(new Shape(shape));
		}

		// convert tensor.
		switch (targetDataType) {
			case DOUBLE:
				tensor = DoubleTensor.of(tensor);
				break;
			case FLOAT:
				tensor = FloatTensor.of(tensor);
				break;
			default:
				// pass
		}

		if (!(tensor.getType().equals(targetDataType))) {
			switch (handleInvalidMethod) {
				case ERROR:
					throw new IllegalArgumentException(
						String.format("Could not convert tensor %s to tensor type %s", tensor, targetDataType)
					);
				case SKIP:
					tensor = null;
					break;
				default:
					throw new UnsupportedOperationException();
			}
		}

		if (tensor == null) {
			return null;
		}

		if (shape == null) {
			return tensor;
		}

		return tensor.reshape(new Shape(shape));
	}

	@Override
	protected TypeInformation <?> initOutputColType() {
		DataType targetDataType = null;

		if (params.contains(ToTensorParams.TENSOR_DATA_TYPE)) {
			targetDataType = params.get(ToTensorParams.TENSOR_DATA_TYPE);
		}

		if (targetDataType == null) {
			return AlinkTypes.TENSOR;
		}

		switch (targetDataType) {
			case FLOAT:
				return AlinkTypes.FLOAT_TENSOR;
			case DOUBLE:
				return AlinkTypes.DOUBLE_TENSOR;
			case STRING:
				return AlinkTypes.STRING_TENSOR;
			case INT:
				return AlinkTypes.INT_TENSOR;
			case LONG:
				return AlinkTypes.LONG_TENSOR;
			case BOOLEAN:
				return AlinkTypes.BOOL_TENSOR;
			case UBYTE:
				return AlinkTypes.UBYTE_TENSOR;
			case BYTE:
				return AlinkTypes.BYTE_TENSOR;
			default:
				throw new IllegalArgumentException("Unsupported tensor data type: " + targetDataType);
		}
	}
}
