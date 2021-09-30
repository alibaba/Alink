package com.alibaba.alink.operator.common.tensorflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.JsonConverter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TFModelDataConverterUtils {

	public static ParamInfo <String[]> TF_INPUT_COLS = ParamInfoFactory
		.createParamInfo("tfInputCols", String[].class)
		.setDescription("tfInputCols")
		.build();

	public static ParamInfo <String> TF_OUTPUT_SIGNATURE_DEF = ParamInfoFactory
		.createParamInfo("tfOutputSignatureDef", String.class)
		.setDescription("tfOutputSignatureDef")
		.build();

	public static ParamInfo <TypeInformation> TF_OUTPUT_SIGNATURE_TYPE = ParamInfoFactory
		.createParamInfo("tfOutputSignatureType", TypeInformation.class)
		.setDescription("tfOutputSignatureType")
		.build();

	public static ParamInfo <Long> TF_MODEL_PARTITION_START = ParamInfoFactory
		.createParamInfo("tfModelPartitionStart", Long.class)
		.setDescription("tfModelPartitionStart")
		.build();

	public static ParamInfo <Long> TF_MODEL_PARTITION_SIZE = ParamInfoFactory
		.createParamInfo("tfModelPartitionSize", Long.class)
		.setDescription("tfModelPartitionSize")
		.build();

	public static ParamInfo <String> PREPROCESS_PIPELINE_MODEL_SCHEMA_STR = ParamInfoFactory
		.createParamInfo("preprocessPipelineModelSchemaStr", String.class)
		.setDescription("preprocessPipelineModelSchemaStr")
		.setHasDefaultValue(null)
		.build();

	public static ParamInfo <Long> PREPROCESS_PIPELINE_MODEL_PARTITION_START = ParamInfoFactory
		.createParamInfo("preprocessPipelineModelPartitionStart", Long.class)
		.setDescription("preprocessPipelineModelPartitionStart")
		.build();

	public static ParamInfo <Long> PREPROCESS_PIPELINE_MODEL_PARTITION_SIZE = ParamInfoFactory
		.createParamInfo("preprocessPipelineModelPartitionSize", Long.class)
		.setDescription("preprocessPipelineModelPartitionSize")
		.build();

	public static long appendModelRows(List <Row> modelRows, List <String> toAppend) {
		if (modelRows == null || modelRows.size() == 0) {
			return 0;
		}
		int arity = modelRows.get(0).getArity();
		Object[] objects = new Object[arity];
		for (Row row : modelRows) {
			for (int i = 0; i < arity; i += 1) {
				objects[i] = row.getField(i);
			}
			toAppend.add(JsonConverter.toJson(objects));
		}
		return modelRows.size();
	}

	public static List <Row> extractModelRows(Iterator <String> iterator, long size) {
		List <Row> modelRows = new ArrayList <>();
		Object[] objects;
		for (long k = 0; k < size; k += 1) {
			String s = iterator.next();
			objects = JsonConverter.fromJson(s, Object[].class);
			int arity = objects.length;
			Row row = new Row(arity);
			for (int i = 0; i < arity; i += 1) {
				row.setField(i, objects[i]);
			}
			// Json cannot keep types, need to convert first column from Integer to Long
			row.setField(0, ((Integer) row.getField(0)).longValue());
			modelRows.add(row);
		}
		return modelRows;
	}
}
