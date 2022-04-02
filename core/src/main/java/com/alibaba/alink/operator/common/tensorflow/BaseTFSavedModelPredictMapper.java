package com.alibaba.alink.operator.common.tensorflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.dl.plugin.DLPredictorService;
import com.alibaba.alink.common.dl.plugin.TFPredictorClassLoaderFactory;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dl.HasModelPath;
import com.alibaba.alink.params.tensorflow.savedmodel.BaseTFSavedModelPredictParams;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.GRAPH_DEF_TAG_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.INPUT_SIGNATURE_DEFS_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.INTER_OP_PARALLELISM_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.INTRA_OP_PARALLELISM_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.MODEL_PATH_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.OUTPUT_SIGNATURE_DEFS_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.OUTPUT_TYPE_CLASSES;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.SIGNATURE_DEF_KEY_KEY;

/**
 * The base classes of all SavedModel predictors which inputs and outputs data include the batch dimension.
 * <p>
 * `modelPath` must be a local path which is obtained from `params` or setter.
 * <p>
 * NOTE: Previous implementation use strings as the inputs and outputs. Current requirements of the inclusion of the
 * batch dimension is compatible with previous implementation in most cases, except when wrong shape specified in the
 * input strings.
 */
public class BaseTFSavedModelPredictMapper extends Mapper implements Serializable {

	private final TFPredictorClassLoaderFactory factory;

	protected final String[] tfOutputCols;
	protected final Class <?>[] tfOutputColTypeClasses;
	private final String graphDefTag;
	private final String signatureDefKey;
	protected String[] tfInputCols;
	protected DLPredictorService predictor;
	private String[] inputSignatureDefs;
	private String[] outputSignatureDefs;
	private String modelPath;

	public BaseTFSavedModelPredictMapper(TableSchema dataSchema, Params params) {
		this(dataSchema, params, new TFPredictorClassLoaderFactory());
	}

	public BaseTFSavedModelPredictMapper(TableSchema dataSchema, Params params,
										 TFPredictorClassLoaderFactory factory) {
		super(dataSchema, params);

		this.factory = factory;

		graphDefTag = params.get(BaseTFSavedModelPredictParams.GRAPH_DEF_TAG);
		signatureDefKey = params.get(BaseTFSavedModelPredictParams.SIGNATURE_DEF_KEY);

		tfInputCols = params.get(BaseTFSavedModelPredictParams.SELECTED_COLS);
		if (null == tfInputCols) {
			tfInputCols = dataSchema.getFieldNames();
		}
		inputSignatureDefs = params.get(BaseTFSavedModelPredictParams.INPUT_SIGNATURE_DEFS);
		if (null == inputSignatureDefs) {
			inputSignatureDefs = tfInputCols;
		}

		Preconditions.checkArgument(params.contains(BaseTFSavedModelPredictParams.OUTPUT_SCHEMA_STR),
			"Must set outputSchemaStr.");
		String tfOutputSchemaStr = params.get(BaseTFSavedModelPredictParams.OUTPUT_SCHEMA_STR);
		TableSchema tfOutputSchema = TableUtil.schemaStr2Schema(tfOutputSchemaStr);
		tfOutputCols = tfOutputSchema.getFieldNames();
		outputSignatureDefs = params.get(BaseTFSavedModelPredictParams.OUTPUT_SIGNATURE_DEFS);
		if (null == outputSignatureDefs) {
			outputSignatureDefs = tfOutputCols;
		}
		TypeInformation <?>[] tfOutputColTypes = tfOutputSchema.getFieldTypes();
		tfOutputColTypeClasses = Arrays.stream(tfOutputColTypes)
			.map(TypeInformation::getTypeClass)
			.toArray(Class[]::new);

		if (params.contains(HasModelPath.MODEL_PATH)) {
			modelPath = params.get(HasModelPath.MODEL_PATH);
		}
	}

	public BaseTFSavedModelPredictMapper setModelPath(String modelPath) {
		this.modelPath = modelPath;
		return this;
	}

	protected Map <String, Object> getPredictorConfig() {
		Map <String, Object> config = new HashMap <>();
		Integer intraOpParallelism = params.contains(BaseTFSavedModelPredictParams.INTRA_OP_PARALLELISM)
			? params.get(BaseTFSavedModelPredictParams.INTRA_OP_PARALLELISM)
			: null;
		config.put(MODEL_PATH_KEY, modelPath);
		config.put(GRAPH_DEF_TAG_KEY, graphDefTag);
		config.put(SIGNATURE_DEF_KEY_KEY, signatureDefKey);
		config.put(INPUT_SIGNATURE_DEFS_KEY, inputSignatureDefs);
		config.put(OUTPUT_SIGNATURE_DEFS_KEY, outputSignatureDefs);
		config.put(OUTPUT_TYPE_CLASSES, tfOutputColTypeClasses);
		config.put(INTRA_OP_PARALLELISM_KEY, intraOpParallelism);
		config.put(INTER_OP_PARALLELISM_KEY, null);
		return config;
	}

	@Override
	public void open() {
		Preconditions.checkArgument(modelPath != null, "Model path is not set.");
		predictor = TFPredictorClassLoaderFactory.create(factory);
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
		String[] tfInputCols = params.get(BaseTFSavedModelPredictParams.SELECTED_COLS);
		if (null == tfInputCols) {
			tfInputCols = dataSchema.getFieldNames();
		}
		String tfOutputSchemaStr = params.get(BaseTFSavedModelPredictParams.OUTPUT_SCHEMA_STR);
		TableSchema tfOutputSchema = TableUtil.schemaStr2Schema(tfOutputSchemaStr);
		String[] reservedCols = params.get(BaseTFSavedModelPredictParams.RESERVED_COLS);
		return Tuple4.of(tfInputCols,
			tfOutputSchema.getFieldNames(),
			tfOutputSchema.getFieldTypes(),
			reservedCols);
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
