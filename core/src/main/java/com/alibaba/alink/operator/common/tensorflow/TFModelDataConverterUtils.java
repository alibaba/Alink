package com.alibaba.alink.operator.common.tensorflow;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.classification.tensorflow.TFTableModelClassificationModelData;
import com.alibaba.alink.operator.common.regression.tensorflow.TFTableModelRegressionModelData;
import com.google.common.collect.Iterables;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Iterator;
import java.util.List;

public class TFModelDataConverterUtils {

	private static final ParamInfo <Long> TF_MODEL_PARTITION_START = ParamInfoFactory
		.createParamInfo("tfModelPartitionStart", Long.class)
		.setDescription("tfModelPartitionStart")
		.build();

	private static final ParamInfo <Long> TF_MODEL_PARTITION_SIZE = ParamInfoFactory
		.createParamInfo("tfModelPartitionSize", Long.class)
		.setDescription("tfModelPartitionSize")
		.build();

	private static final ParamInfo <String> PREPROCESS_PIPELINE_MODEL_SCHEMA_STR = ParamInfoFactory
		.createParamInfo("preprocessPipelineModelSchemaStr", String.class)
		.setDescription("preprocessPipelineModelSchemaStr")
		.setHasDefaultValue(null)
		.build();

	private static final ParamInfo <Long> PREPROCESS_PIPELINE_MODEL_PARTITION_START = ParamInfoFactory
		.createParamInfo("preprocessPipelineModelPartitionStart", Long.class)
		.setDescription("preprocessPipelineModelPartitionStart")
		.build();

	private static final ParamInfo <Long> PREPROCESS_PIPELINE_MODEL_PARTITION_SIZE = ParamInfoFactory
		.createParamInfo("preprocessPipelineModelPartitionSize", Long.class)
		.setDescription("preprocessPipelineModelPartitionSize")
		.build();

	public static ParamInfo <String[]> TF_INPUT_COLS = ParamInfoFactory
		.createParamInfo("tfInputCols", String[].class)
		.setDescription("tfInputCols")
		.build();

	public static ParamInfo <String> TF_OUTPUT_SIGNATURE_DEF = ParamInfoFactory
		.createParamInfo("tfOutputSignatureDef", String.class)
		.setDescription("tfOutputSignatureDef")
		.build();

	public static ParamInfo <Boolean> IS_OUTPUT_LOGITS = ParamInfoFactory
		.createParamInfo("isOutputLogits", Boolean.class)
		.setDescription("isOutputLogits")
		.setHasDefaultValue(false)
		.build();

	public static Tuple3 <Params, Iterable <String>, Iterable <Object>> serializeRegressionModel(
		TFTableModelRegressionModelData modelData) {
		Params meta = modelData.getMeta().clone();
		meta.set(TF_INPUT_COLS, modelData.getTfInputCols());
		meta.set(TF_OUTPUT_SIGNATURE_DEF, modelData.getTfOutputSignatureDef());
		//meta.set(TFModelDataConverterUtils.TF_OUTPUT_SIGNATURE_TYPE, modelData.getTfOutputSignatureType());
		meta.set(PREPROCESS_PIPELINE_MODEL_SCHEMA_STR,
			modelData.getPreprocessPipelineModelSchemaStr());

		// TF model data is Iterable <Row> whose size is unknown, so it can only be put at the last.
		Iterable <Row> tfModelSerialized = modelData.getTfModelRows();
		List <Row> preprocessPipelineModelRows = modelData.getPreprocessPipelineModelRows();
		ModelRowsIterable data = new ModelRowsIterable(preprocessPipelineModelRows, tfModelSerialized);

		long pStart = 0, pSize;
		pSize = preprocessPipelineModelRows.size();
		meta.set(PREPROCESS_PIPELINE_MODEL_PARTITION_START, pStart);
		meta.set(PREPROCESS_PIPELINE_MODEL_PARTITION_SIZE, pSize);
		pStart += pSize;

		// Unable to get TF_MODEL_PARTITION_SIZE, so use TF_MODEL_PARTITION_SIZE to distinguish the new storage from the old one.
		meta.set(TF_MODEL_PARTITION_START, pStart);

		return Tuple3.of(meta, data, new ArrayList <>());
	}

	public static Tuple3 <Params, Iterable <String>, Iterable <Object>> serializeClassificationModel(
		TFTableModelClassificationModelData modelData) {
		Tuple3 <Params, Iterable <String>, Iterable <Object>> tuple3 = serializeRegressionModel(modelData);
		Params meta = tuple3.f0;
		Iterable <String> data = tuple3.f1;
		meta.set(IS_OUTPUT_LOGITS, modelData.getIsLogits());
		Iterable <Object> labels = modelData.getSortedLabels();
		return Tuple3.of(meta, data, labels);
	}

	// Only use for non-TF model data
	private static List <Row> extractModelRows(Iterator <String> iterator, long size) {
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

	private static String writeModelRowsToFile(Iterator <String> iterator, long size) {
		Path workPath = PythonFileUtils.createTempDir("saved_model_");
		String s = iterator.next();
		Object[] objects = JsonConverter.fromJson(s, Object[].class);
		String zipFilename = (String) objects[1];
		Path zipPath = workPath.resolve(zipFilename);

		Decoder decoder = Base64.getDecoder();
		try (FileOutputStream fos = new FileOutputStream(zipPath.toFile())) {
			for (long k = 1; k < size; k += 1) {
				s = iterator.next();
				objects = JsonConverter.fromJson(s, Object[].class);
				fos.write(decoder.decode((String) objects[1]));
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return zipPath.toFile().getAbsolutePath();
	}

	private static String writeModelRowsToFile(Iterator <String> iterator) {
		Path workPath = PythonFileUtils.createTempDir("saved_model_");
		String s = iterator.next();
		Object[] objects = JsonConverter.fromJson(s, Object[].class);
		String zipFilename = (String) objects[1];
		Path zipPath = workPath.resolve(zipFilename);

		Decoder decoder = Base64.getDecoder();
		try (FileOutputStream fos = new FileOutputStream(zipPath.toFile())) {
			while (iterator.hasNext()) {
				s = iterator.next();
				objects = JsonConverter.fromJson(s, Object[].class);
				fos.write(decoder.decode((String) objects[1]));
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return zipPath.toFile().getAbsolutePath();
	}

	public static void deserializeRegressionModel(TFTableModelRegressionModelData modelData,
												  Params meta, Iterable <String> data) {
		modelData.setMeta(meta);
		modelData.setTfInputCols(meta.get(TF_INPUT_COLS));
		modelData.setPreprocessPipelineModelSchemaStr(meta.get(PREPROCESS_PIPELINE_MODEL_SCHEMA_STR));

		boolean isLegacyFormat = meta.contains(TF_MODEL_PARTITION_SIZE);
		Iterator <String> iterator = data.iterator();
		if (isLegacyFormat) {
			String zipFilePath = writeModelRowsToFile(iterator, meta.get(TF_MODEL_PARTITION_SIZE));
			modelData.setTfModelZipPath(zipFilePath);

			if (meta.contains(PREPROCESS_PIPELINE_MODEL_PARTITION_SIZE)) {
				List <Row> preprocessPipelineModelSerialized = extractModelRows(iterator,
					meta.get(PREPROCESS_PIPELINE_MODEL_PARTITION_SIZE));
				modelData.setPreprocessPipelineModelRows(preprocessPipelineModelSerialized);
			}
		} else {
			if (meta.contains(PREPROCESS_PIPELINE_MODEL_PARTITION_SIZE)) {
				List <Row> preprocessPipelineModelSerialized = extractModelRows(iterator,
					meta.get(PREPROCESS_PIPELINE_MODEL_PARTITION_SIZE));
				modelData.setPreprocessPipelineModelRows(preprocessPipelineModelSerialized);
			}
			String zipFilePath = writeModelRowsToFile(iterator);
			modelData.setTfModelZipPath(zipFilePath);
		}
	}

	public static void deserializeClassificationModel(TFTableModelClassificationModelData modelData,
													  Params meta, Iterable <String> data,
													  Iterable <Object> distinctLabels) {
		deserializeRegressionModel(modelData, meta, data);
		modelData.setSortedLabels(distinctLabels);
	}

	static class ModelRowsIterable implements Iterable <String> {
		private final Iterable <Row> concat;

		@SafeVarargs
		public ModelRowsIterable(Iterable <Row>... iterables) {
			concat = Iterables.concat(iterables);
		}

		@Override
		public ModelRowsIterator iterator() {
			return new ModelRowsIterator(concat.iterator());
		}

		static class ModelRowsIterator implements Iterator <String> {
			private final Iterator <Row> iter;

			public ModelRowsIterator(Iterator <Row> iter) {
				this.iter = iter;
			}

			@Override
			public boolean hasNext() {
				return iter.hasNext();
			}

			@Override
			public String next() {
				Row row = iter.next();
				Object[] objects = new Object[row.getArity()];
				for (int i = 0; i < row.getArity(); i += 1) {
					objects[i] = row.getField(i);
				}
				return JsonConverter.toJson(objects);
			}
		}
	}
}
