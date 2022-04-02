package com.alibaba.alink.operator.common.io.csv;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.linalg.VectorType;
import com.alibaba.alink.common.linalg.tensor.DataType;
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
			if (AlinkTypes.isTensorType(colTypes[i])
				|| AlinkTypes.isVectorType(colTypes[i])
				|| AlinkTypes.isMTableType(colTypes[i])) {

				rewriteColTypes[i] = Types.STRING;
			} else {
				rewriteColTypes[i] = colTypes[i];
			}
		}

		return rewriteColTypes;
	}

	public static DataType tensorTypeInformationToTensorType(TypeInformation <?> typeInformation) {

		if (AlinkTypes.TENSOR.equals(typeInformation)) {
			return null;
		}

		if (AlinkTypes.FLOAT_TENSOR.equals(typeInformation)) {
			return DataType.FLOAT;
		}

		if (AlinkTypes.DOUBLE_TENSOR.equals(typeInformation)) {
			return DataType.DOUBLE;
		}

		if (AlinkTypes.INT_TENSOR.equals(typeInformation)) {
			return DataType.INT;
		}

		if (AlinkTypes.LONG_TENSOR.equals(typeInformation)) {
			return DataType.LONG;
		}

		if (AlinkTypes.STRING_TENSOR.equals(typeInformation)) {
			return DataType.STRING;
		}

		if (AlinkTypes.BYTE_TENSOR.equals(typeInformation)) {
			return DataType.BYTE;
		}

		if (AlinkTypes.UBYTE_TENSOR.equals(typeInformation)) {
			return DataType.UBYTE;
		}

		if (AlinkTypes.BOOL_TENSOR.equals(typeInformation)) {
			return DataType.BOOLEAN;
		}

		throw new IllegalArgumentException("Unsupported tensor type. " + typeInformation);
	}

	public static PipelineModel toTensorPipelineModel(
		Params params, String[] colNames, TypeInformation <?>[] colTypes) {

		List <TransformerBase <?>> toTensorList = new ArrayList <>();

		for (int i = 0; i < colTypes.length; ++i) {
			if (AlinkTypes.isTensorType(colTypes[i])) {

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
		if (AlinkTypes.VECTOR.equals(typeInformation)) {
			return null;
		}

		if (AlinkTypes.DENSE_VECTOR.equals(typeInformation)) {
			return VectorType.DENSE;
		}

		if (AlinkTypes.SPARSE_VECTOR.equals(typeInformation)) {
			return VectorType.SPARSE;
		}

		throw new IllegalArgumentException("Unsupported vector type. " + typeInformation);
	}

	public static PipelineModel toVectorPipelineModel(
		Params params, String[] colNames, TypeInformation <?>[] colTypes) {

		List <TransformerBase <?>> toVectorList = new ArrayList <>();

		for (int i = 0; i < colTypes.length; ++i) {
			if (AlinkTypes.isVectorType(colTypes[i])) {

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
			if (AlinkTypes.isMTableType(colTypes[i])) {
				toMTableList.add(new ToMTable(params.clone().set(ToTensorParams.SELECTED_COL, colNames[i])));
			}
		}

		return new PipelineModel(toMTableList.toArray(new TransformerBase[0]));
	}

}
