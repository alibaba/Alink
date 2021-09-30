package com.alibaba.alink.operator.common.tensorflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.tensorflow.savedmodel.HasInputNames;
import com.alibaba.alink.params.tensorflow.savedmodel.HasOutputNames;
import com.alibaba.alink.params.tensorflow.savedmodel.TFSavedModelPredictParams;

import java.util.Arrays;

/**
 * Predict mapper for SavedModel. Different from {@link BaseTFSavedModelPredictRowMapper}, `modelPath` can be oss links
 * or http/https links. Different from {@link TFSavedModelPredictMapper}, inputs and outputs do not include batch
 * dimension.
 */
public class TFSavedModelPredictRowMapper extends BaseTFSavedModelPredictMapper {

	private final BaseTFSavedModelPredictRowMapper mapper;

	public TFSavedModelPredictRowMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);

		Params mapperParams = params.clone();

		// Compatible with previous behaviors, where selected columns are equal to input names.
		if (params.contains(HasInputNames.INPUT_NAMES)) {
			String[] inputNames = params.get(HasInputNames.INPUT_NAMES);
			mapperParams.set(TFSavedModelPredictParams.SELECTED_COLS, inputNames);
		}

		// Compatible with previous behaviors, where output columns are equal to output names, and types are strings.
		if (params.contains(HasOutputNames.OUTPUT_NAMES)) {
			String[] outputNames = params.get(HasOutputNames.OUTPUT_NAMES);
			TypeInformation <?>[] outputTypes = new TypeInformation[outputNames.length];
			Arrays.fill(outputTypes, Types.STRING);
			TableSchema outputSchema = new TableSchema(outputNames, outputTypes);
			mapperParams.set(TFSavedModelPredictParams.OUTPUT_SCHEMA_STR, CsvUtil.schema2SchemaStr(outputSchema));
		}

		mapper = new BaseTFSavedModelPredictRowMapper(dataSchema, mapperParams);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema dataSchema,
																						   Params params) {
		String[] tfInputCols = params.get(TFSavedModelPredictParams.SELECTED_COLS);
		if (null == tfInputCols) {
			tfInputCols = dataSchema.getFieldNames();
		}
		String tfOutputSchemaStr = params.get(TFSavedModelPredictParams.OUTPUT_SCHEMA_STR);
		TableSchema tfOutputSchema = CsvUtil.schemaStr2Schema(tfOutputSchemaStr);
		String[] reservedCols = params.get(TFSavedModelPredictParams.RESERVED_COLS);
		return Tuple4.of(tfInputCols,
			tfOutputSchema.getFieldNames(),
			tfOutputSchema.getFieldTypes(),
			reservedCols);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		mapper.map(selection, result);
	}

	@Override
	public void open() {
		String modelPath = params.get(TFSavedModelPredictParams.MODEL_PATH);
		String localModelPath = TFSavedModelUtils.downloadSavedModel(modelPath);
		mapper.setModelPath(localModelPath);
		mapper.open();
	}

	@Override
	public void close() {
		mapper.close();
	}
}
