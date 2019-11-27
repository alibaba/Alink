package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.common.dataproc.StringIndexerModelMapper;
import com.alibaba.alink.params.dataproc.StringIndexerPredictParams;
import com.alibaba.alink.pipeline.MapModel;
import org.apache.flink.ml.api.misc.param.Params;

import java.util.HashMap;
import java.util.Map;

/**
 * Model fitted by {@link StringIndexer}.
 * <p>
 * <p>The model transforms one column of strings to indices based on the model
 * fitted by StringIndexer.
 */
public class StringIndexerModel extends MapModel<StringIndexerModel>
    implements StringIndexerPredictParams<StringIndexerModel> {

    private static Map<String, StringIndexerModel> registeredModel = new HashMap<>();

    public StringIndexerModel(Params params) {
        super(StringIndexerModelMapper::new, params);
        if (params.contains(StringIndexer.MODEL_NAME)) {
            registerModel(params.get(StringIndexer.MODEL_NAME), this);
        }
    }

    /**
     * Bookkeeping the model, so that it could be referenced by {@link IndexToString} by name.
     *
     * @param modelName Name of the model.
     * @param model     The model to register.
     */
    private static synchronized void registerModel(String modelName, StringIndexerModel model) {
        registeredModel.put(modelName, model);
    }

    /**
     * Get the registered model through name.
     *
     * @param modelName The name of the model.
     * @return The model.
     */
    static synchronized StringIndexerModel getRegisteredModel(String modelName) {
        StringIndexerModel model = registeredModel.get(modelName);
        if (model == null) {
            throw new RuntimeException("Can't find StringIndexerModel with name: " + modelName);
        }
        return model;
    }
}