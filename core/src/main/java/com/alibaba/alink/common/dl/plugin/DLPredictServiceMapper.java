package com.alibaba.alink.common.dl.plugin;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dl.HasModelPath;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;
import com.alibaba.alink.params.tensorflow.savedmodel.HasOutputSchemaStr;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class DLPredictServiceMapper<FACTORY> extends Mapper implements Serializable {

	protected final FACTORY factory;
	protected final String[] outputCols;
	protected final Class <?>[] outputColTypeClasses;
	protected String[] inputCols;
	protected String modelPath;
	protected DLPredictorService predictor;

	public DLPredictServiceMapper(TableSchema dataSchema, Params params, FACTORY factory) {
		super(dataSchema, params);

		this.factory = factory;

		inputCols = params.get(HasSelectedColsDefaultAsNull.SELECTED_COLS);
		if (null == inputCols) {
			inputCols = dataSchema.getFieldNames();
		}

		Preconditions.checkArgument(params.contains(HasOutputSchemaStr.OUTPUT_SCHEMA_STR),
			"Must set outputSchemaStr.");
		String outputSchemaStr = params.get(HasOutputSchemaStr.OUTPUT_SCHEMA_STR);
		TableSchema outputSchema = TableUtil.schemaStr2Schema(outputSchemaStr);
		outputCols = outputSchema.getFieldNames();

		TypeInformation <?>[] outputColTypes = outputSchema.getFieldTypes();
		outputColTypeClasses = Arrays.stream(outputColTypes)
			.map(TypeInformation::getTypeClass)
			.toArray(Class[]::new);

		if (params.contains(HasModelPath.MODEL_PATH)) {
			modelPath = params.get(HasModelPath.MODEL_PATH);
		}
	}

	public DLPredictServiceMapper <FACTORY> setModelPath(String modelPath) {
		this.modelPath = modelPath;
		return this;
	}

	protected abstract Map <String, Object> getPredictorConfig();

	@Override
	public void open() {
		Preconditions.checkArgument(modelPath != null, "Model path is not set.");
		try {
			Method createMethod = factory.getClass().getMethod("create", factory.getClass());
			predictor = (DLPredictorService) createMethod.invoke(null, factory);
		} catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
			throw new RuntimeException(
				String.format("Failed to call %s#create(factory).", factory.getClass().getCanonicalName()), e);
		}
		predictor.open(getPredictorConfig());
	}

	@Override
	public void close() {
		try {
			predictor.close();
		} catch (Exception e) {
			throw new RuntimeException("Failed to close predictor", e);
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema dataSchema,
																						   Params params) {
		String[] inputCols = params.get(HasSelectedColsDefaultAsNull.SELECTED_COLS);
		if (null == inputCols) {
			inputCols = dataSchema.getFieldNames();
		}
		String outputSchemaStr = params.get(HasOutputSchemaStr.OUTPUT_SCHEMA_STR);
		TableSchema outputSchema = TableUtil.schemaStr2Schema(outputSchemaStr);
		String[] reservedCols = params.get(HasReservedColsDefaultAsNull.RESERVED_COLS);
		return Tuple4.of(inputCols, outputSchema.getFieldNames(), outputSchema.getFieldTypes(), reservedCols);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		List <Object> inputs = new ArrayList <>();
		for (int i = 0; i < selection.length(); i += 1) {
			inputs.add(selection.get(i));
		}
		List <?> outputs = predictor.predict(inputs);
		for (int i = 0; i < result.length(); i += 1) {
			result.set(i, outputs.get(i));
		}
	}
}
