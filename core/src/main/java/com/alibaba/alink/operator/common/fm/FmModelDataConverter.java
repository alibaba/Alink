package com.alibaba.alink.operator.common.fm;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.model.ModelDataConverter;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.FmDataFormat;

import java.util.List;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

/**
 * Fm model converter. This converter can help serialize and deserialize the model data.
 */
public class FmModelDataConverter implements ModelDataConverter<FmModelData, FmModelData> {

    /**
     * Data type of labels.
     */
    protected TypeInformation labelType;

    public FmModelDataConverter(TypeInformation labelType) {
        this.labelType = labelType;
    }

    public FmModelDataConverter() {
    }

    @Override
    public void save(FmModelData modelData, Collector<Row> collector) {
        Params meta = new Params()
                .set(ModelParamName.VECTOR_COL_NAME, modelData.vectorColName)
                .set(ModelParamName.LABEL_COL_NAME, modelData.labelColName)
                .set(ModelParamName.TASK, modelData.task)
                .set(ModelParamName.VECTOR_SIZE, modelData.vectorSize)
                .set(ModelParamName.FEATURE_COL_NAMES, modelData.featureColNames)
                .set(ModelParamName.LABEL_VALUES, modelData.labelValues)
                .set(ModelParamName.DIM, modelData.dim)
                .set(ModelParamName.REGULAR, modelData.regular)
                .set(ModelParamName.LOSS_CURVE, modelData.convergenceInfo);
        FmDataFormat factors = modelData.fmModel;

        collector.collect(Row.of(null, meta.toJson(), null));

        for (int i = 0; i < factors.factors.length; ++i) {
            double[] factor = factors.factors[i];
            collector.collect(Row.of((long)i, JsonConverter.toJson(factor), null));
        }
        collector.collect(Row.of(-1L, JsonConverter.toJson(new double[]{factors.bias}), null));
    }

    @Override
    public FmModelData load(List<Row> rows) {
        Params meta = null;
        for (Row row : rows) {
            Long featureId = (Long) row.getField(0);
            if (featureId == null && row.getField(1) != null) {
                String metaStr = (String) row.getField(1);
                meta = Params.fromJson(metaStr);
                break;
            }
        }

        FmModelData modelData = new FmModelData();
        assert meta != null;
        modelData.vectorColName = meta.get(ModelParamName.VECTOR_COL_NAME);
        modelData.featureColNames = meta.get(ModelParamName.FEATURE_COL_NAMES);
        modelData.labelColName = meta.get(ModelParamName.LABEL_COL_NAME);
        modelData.task = meta.get(ModelParamName.TASK);
        modelData.dim = meta.get(ModelParamName.DIM);
        modelData.regular = meta.contains(ModelParamName.REGULAR) ? meta.get(ModelParamName.REGULAR) : null;
        modelData.vectorSize = meta.get(ModelParamName.VECTOR_SIZE);
        modelData.convergenceInfo = meta.get(ModelParamName.LOSS_CURVE);

        if (meta.contains(ModelParamName.LABEL_VALUES)) {
            modelData.labelValues = meta.get(ModelParamName.LABEL_VALUES);
        }
        modelData.fmModel = new FmDataFormat();
        modelData.fmModel.factors = new double[modelData.vectorSize][];
        modelData.fmModel.dim = modelData.dim;
        for (Row row : rows) {
            Long featureId = (Long) row.getField(0);
            if (featureId != null) {
                if (featureId >= 0) {
                    double[] factor = gson.fromJson((String) row.getField(1), double[].class);
                    modelData.fmModel.factors[featureId.intValue()] = factor;
                } else if (featureId == -1) {
                    double[] factor = gson.fromJson((String) row.getField(1), double[].class);
                    modelData.fmModel.bias = factor[0];
                }
            }
        }
        return modelData;
    }

    /**
     * A utility function to extract label type from model schema.
     */
    public static TypeInformation extractLabelType(TableSchema modelSchema) {
        return modelSchema.getFieldTypes()[2];
    }

    @Override
    public TableSchema getModelSchema() {
        String[] modelColNames = new String[] {"feature_id", "feature_weights", "label_type"};
        TypeInformation[] modelColTypes = new TypeInformation[] {Types.LONG, Types.STRING, labelType};
        return new TableSchema(modelColNames, modelColTypes);
    }
}
