package com.alibaba.alink.operator.common.io.csv;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.MTableTypes;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.VectorType;
import com.alibaba.alink.common.linalg.tensor.DataType;
import com.alibaba.alink.common.linalg.tensor.TensorTypes;
import com.alibaba.alink.params.dataproc.ToTensorParams;
import com.alibaba.alink.params.dataproc.ToVectorParams;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.TransformerBase;
import com.alibaba.alink.pipeline.dataproc.ToMTable;
import com.alibaba.alink.pipeline.dataproc.ToTensor;
import com.alibaba.alink.pipeline.dataproc.ToVector;

import java.util.ArrayList;
import java.util.List;

public class CsvTypeConverter {

	public static TypeInformation <?>[] rewriteColTypes(TypeInformation <?>[] colTypes) {
		TypeInformation <?>[] rewriteColTypes = new TypeInformation <?>[colTypes.length];

		// rewrite schema
		for (int i = 0; i < colTypes.length; ++i) {
			if (TensorTypes.isTensorType(colTypes[i])
				|| VectorTypes.isVectorType(colTypes[i])
				|| MTableTypes.isMTableType(colTypes[i])) {

				rewriteColTypes[i] = Types.STRING;
			} else {
				rewriteColTypes[i] = colTypes[i];
			}
		}

		return rewriteColTypes;
	}

	public static DataType tensorTypeInformationToTensorType(TypeInformation <?> typeInformation) {

		if (TensorTypes.TENSOR.equals(typeInformation)) {
			return null;
		}

		if (TensorTypes.FLOAT_TENSOR.equals(typeInformation)) {
			return DataType.FLOAT;
		}

		if (TensorTypes.DOUBLE_TENSOR.equals(typeInformation)) {
			return DataType.DOUBLE;
		}

		if (TensorTypes.INT_TENSOR.equals(typeInformation)) {
			return DataType.INT;
		}

		if (TensorTypes.LONG_TENSOR.equals(typeInformation)) {
			return DataType.LONG;
		}

		if (TensorTypes.STRING_TENSOR.equals(typeInformation)) {
			return DataType.STRING;
		}

		if (TensorTypes.BYTE_TENSOR.equals(typeInformation)) {
			return DataType.BYTE;
		}

		if (TensorTypes.UBYTE_TENSOR.equals(typeInformation)) {
			return DataType.UBYTE;
		}

		if (TensorTypes.BOOL_TENSOR.equals(typeInformation)) {
			return DataType.BOOLEAN;
		}

		throw new IllegalArgumentException("Unsupported tensor type. " + typeInformation);
	}

	public static PipelineModel toTensorPipelineModel(
		Params params, String[] colNames, TypeInformation <?>[] colTypes) {

		List <TransformerBase <?>> toTensorList = new ArrayList <>();

		for (int i = 0; i < colTypes.length; ++i) {
			if (TensorTypes.isTensorType(colTypes[i])) {

				Params localParam = params.clone();

				DataType dataType = tensorTypeInformationToTensorType(colTypes[i]);

				if (dataType != null) {
					localParam.set(ToTensorParams.TENSOR_DATA_TYPE, dataType);
				}

				localParam.set(ToTensorParams.SELECTED_COL, colNames[i]);

				toTensorList.add(new ToTensor(localParam));
			}
		}

		return new PipelineModel(toTensorList.toArray(new TransformerBase[0]));
	}

	public static VectorType vectorTypeInformationToVectorType(TypeInformation <?> typeInformation) {
		if (VectorTypes.VECTOR.equals(typeInformation)) {
			return null;
		}

		if (VectorTypes.DENSE_VECTOR.equals(typeInformation)) {
			return VectorType.DENSE;
		}

		if (VectorTypes.SPARSE_VECTOR.equals(typeInformation)) {
			return VectorType.SPARSE;
		}

		throw new IllegalArgumentException("Unsupported vector type. " + typeInformation);
	}

	public static PipelineModel toVectorPipelineModel(
		Params params, String[] colNames, TypeInformation <?>[] colTypes) {

		List <TransformerBase <?>> toVectorList = new ArrayList <>();

		for (int i = 0; i < colTypes.length; ++i) {
			if (VectorTypes.isVectorType(colTypes[i])) {

				Params localParam = params.clone();

				VectorType vectorType = vectorTypeInformationToVectorType(colTypes[i]);

				if (vectorType != null) {
					localParam.set(ToVectorParams.VECTOR_TYPE, vectorType);
				}

				localParam.set(ToTensorParams.SELECTED_COL, colNames[i]);

				toVectorList.add(new ToVector(localParam));
			}
		}

		return new PipelineModel(toVectorList.toArray(new TransformerBase[0]));
	}

	public static PipelineModel toMTablePipelineModel(
		Params params, String[] colNames, TypeInformation <?>[] colTypes) {

		List <TransformerBase <?>> toMTableList = new ArrayList <>();

		for (int i = 0; i < colTypes.length; ++i) {
			if (MTableTypes.isMTableType(colTypes[i])) {
				toMTableList.add(new ToMTable(params.clone().set(ToTensorParams.SELECTED_COL, colNames[i])));
			}
		}

		return new PipelineModel(toMTableList.toArray(new TransformerBase[0]));
	}

}
