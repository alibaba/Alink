package com.alibaba.alink.operator.common.tensorflow;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.dl.plugin.TFPredictorClassLoaderFactory;
import com.alibaba.alink.params.tensorflow.savedmodel.HasOutputBatchAxes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.OUTPUT_BATCH_AXES;

/**
 * The base classes of SavedModel predictors which inputs and outputs data do not include the batch dimension.
 * <p>
 * `modelPath` must be a local path which is obtained from `params` or setter.
 * <p>
 * The difference compared to {@link BaseTFSavedModelPredictMapper} is that {@link BaseTFSavedModelPredictRowMapper}
 * adds a batch dimension to the inputs and removes the batch dimension from the outputs automatically.
 */
public class BaseTFSavedModelPredictRowMapper extends BaseTFSavedModelPredictMapper {

	private final int[] outputBatchAxes;
	// TODO: support inputBatchAxes

	public BaseTFSavedModelPredictRowMapper(TableSchema dataSchema, Params params) {
		this(dataSchema, params, new TFPredictorClassLoaderFactory());
	}

	public BaseTFSavedModelPredictRowMapper(TableSchema dataSchema, Params params,
											TFPredictorClassLoaderFactory factory) {
		super(dataSchema, params, factory);
		outputBatchAxes = params.contains(HasOutputBatchAxes.OUTPUT_BATCH_AXES)
			? params.get(HasOutputBatchAxes.OUTPUT_BATCH_AXES)
			: new int[tfOutputCols.length];
	}

	@Override
	protected Map <String, Object> getPredictorConfig() {
		Map <String, Object> config = super.getPredictorConfig();
		config.put(OUTPUT_BATCH_AXES, outputBatchAxes);
		return config;
	}

	@Override
	public void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		List <Object> inputs = new ArrayList <>();
		for (int i = 0; i < selection.length(); i += 1) {
			inputs.add(selection.get(i));
		}
		List <?> outputs = predictor.predictRow(inputs);
		for (int i = 0; i < result.length(); i += 1) {
			result.set(i, outputs.get(i));
		}
	}
}
