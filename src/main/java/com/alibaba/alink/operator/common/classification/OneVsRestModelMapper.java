package com.alibaba.alink.operator.common.classification;

import com.alibaba.alink.pipeline.classification.OneVsRest;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.common.classification.ann.SigmoidFunction;
import com.alibaba.alink.operator.common.tree.predictors.GbdtModelMapper;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.operator.common.io.types.JdbcTypeConverter;
import com.alibaba.alink.params.classification.OneVsRestPredictParams;
import com.alibaba.alink.pipeline.classification.GbdtClassifier;
import com.alibaba.alink.pipeline.classification.LinearSvm;
import com.alibaba.alink.pipeline.classification.LogisticRegression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

/**
 * ModelMapper for {@link OneVsRest}.
 */
public class OneVsRestModelMapper extends ModelMapper {

    transient double[] score;
    transient HashMap<Object, Double> details;
    private List<RichModelMapper> predictors;
    private List<Object> labels;
    private Params binClsPredParams;
    private OutputColsHelper outputColsHelper;
    private boolean predDetail;

    public OneVsRestModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);

        String predResultColName = params.get(OneVsRestPredictParams.PREDICTION_COL);
        String[] keepColNames = params.get(OneVsRestPredictParams.RESERVED_COLS);
        this.predDetail = params.contains(OneVsRestPredictParams.PREDICTION_DETAIL_COL);
        int numModelCols = modelSchema.getFieldNames().length;
        TypeInformation labelType = modelSchema.getFieldTypes()[numModelCols - 1];
        if (predDetail) {
            String predDetailColName = params.get(OneVsRestPredictParams.PREDICTION_DETAIL_COL);
            outputColsHelper = new OutputColsHelper(dataSchema, new String[]{predResultColName, predDetailColName},
                new TypeInformation[]{labelType, Types.STRING}, keepColNames);
        } else {
            outputColsHelper = new OutputColsHelper(dataSchema, predResultColName, labelType, keepColNames);
        }

        this.binClsPredParams = params.clone();
        this.binClsPredParams.set(OneVsRestPredictParams.RESERVED_COLS, new String[0]);
        this.binClsPredParams.set(OneVsRestPredictParams.PREDICTION_COL, "pred_result");
        this.binClsPredParams.set(OneVsRestPredictParams.PREDICTION_DETAIL_COL, "pred_detail");
    }

    private static void recoverLabelType(List<Object> labels, String labelTypeName) {
        // after jsonized and un-jsonized, Int, Long, Float objects all changes
        // to Double. So here we recover the label type.

        if (labelTypeName.equals(FlinkTypeConverter.getTypeString(Types.LONG))) {
            for (int i = 0; i < labels.size(); i++) {
                Double label = (Double) labels.get(i);
                labels.set(i, label.longValue());
            }
        } else if (labelTypeName.equals(FlinkTypeConverter.getTypeString(Types.INT))) {
            for (int i = 0; i < labels.size(); i++) {
                Double label = (Double) labels.get(i);
                labels.set(i, label.intValue());
            }
        } else if (labelTypeName.equals(FlinkTypeConverter.getTypeString(Types.FLOAT))) {
            for (int i = 0; i < labels.size(); i++) {
                Double label = (Double) labels.get(i);
                labels.set(i, label.floatValue());
            }
        }
    }

    private static RichModelMapper createModelPredictor(String binClsClssName, TableSchema modelSchema,
														TableSchema dataSchema,
														Params params, List<Row> modelRows) {
        RichModelMapper predictor;
        if (binClsClssName.equals(LogisticRegression.class.getCanonicalName()) ||
            binClsClssName.equals(LinearSvm.class.getCanonicalName())) {
            predictor = new LinearModelMapper(modelSchema, dataSchema, params);
            predictor.loadModel(modelRows);
        } else if (binClsClssName.equals(GbdtClassifier.class.getCanonicalName())) {
            predictor = new GbdtModelMapper(modelSchema, dataSchema, params);
            predictor.loadModel(modelRows);
        } else {
            throw new UnsupportedOperationException("OneVsRest does not support classifier: " + binClsClssName);
        }
        return predictor;
    }

    @Override
    public TableSchema getOutputSchema() {
        return outputColsHelper.getResultSchema();
    }

    public Params extractMeta(List<Row> modelRows) {
        Params meta = null;
        for (Row row : modelRows) {
            if (row.getField(1) != null) {
                meta = Params.fromJson((String) row.getField(1));
                break;
            }
        }
        return meta;
    }

    @Override
    public void loadModel(List<Row> modelRows) {
        Params meta = extractMeta(modelRows);
        int numClasses = meta.get(ModelParamName.NUM_CLASSES);
        String labelsStr = meta.get(ModelParamName.LABELS);
        String labelTypeName = meta.get(ModelParamName.LABEL_TYPE_NAME);
        String binClsClassName = meta.get(ModelParamName.BIN_CLS_CLASS_NAME);
        String[] modelColNames = meta.get(ModelParamName.MODEL_COL_NAMES);
        Integer[] modelColTypesInt = meta.get(ModelParamName.MODEL_COL_TYPES);
        TypeInformation[] modelColTypes = new TypeInformation[modelColTypesInt.length];
        for (int i = 0; i < modelColTypesInt.length; i++) {
            modelColTypes[i] = JdbcTypeConverter.getFlinkType(modelColTypesInt[i]);
        }
        this.predictors = new ArrayList<>(numClasses);
        this.labels = gson.fromJson(labelsStr, ArrayList.class);
        recoverLabelType(labels, labelTypeName);

        try {
            for (int i = 0; i < numClasses; i++) {
                List<Row> rows = new ArrayList<Row>();
                for (Row row : modelRows) {
                    if (row.getField(2) == null) {
                        continue;
                    }
                    long id = (Long) row.getField(2);
                    if ((long) (i) == id) {
                        Row subRow = new Row(row.getArity() - 4);
                        for (int j = 0; j < subRow.getArity(); j++) {
                            subRow.setField(j, row.getField(3 + j));
                        }
                        rows.add(subRow);
                    }
                }
                TableSchema schema = new TableSchema(modelColNames, modelColTypes);
                RichModelMapper predictor = createModelPredictor(binClsClassName, schema, getDataSchema(), binClsPredParams,
                    rows);
                this.predictors.add(predictor);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Row map(Row row) throws Exception {
        Row output = new Row(predDetail ? 2 : 1);
        if (score == null) {
            score = new double[labels.size()];
        }
        if (details == null) {
            details = new HashMap<>(this.predictors.size());
            labels.forEach(label -> {
                details.put(label, 0.0);
            });
        }

        int numClasses = this.predictors.size();
        for (int i = 0; i < numClasses; i++) {
            RichModelMapper predictor = this.predictors.get(i);
            if (predictor instanceof LinearModelMapper) {
                LinearModelMapper linearModelPredictor = (LinearModelMapper) predictor;
                Row result = linearModelPredictor.map(row);
                if (result.getArity() < 2 || result.getField(1) == null) {
                    throw new RuntimeException("no pred detail in the binary classifier");
                }
                Map<String, String> probMap = gson.fromJson((String) result.getField(1), Map.class);
                score[i] = Double.valueOf(probMap.get("1.0"));
            } else if (predictor instanceof GbdtModelMapper) {
                Row result = predictor.map(row);
                score[i] = (Double) result.getField(0);
                score[i] = new SigmoidFunction().eval(score[i]);
            } else {
                throw new UnsupportedOperationException("Not supported predictor: " +
                    predictors.getClass().toString());
            }
        }

        int maxIdx = -1;
        double maxProb = -Double.MAX_VALUE;
        double sum = 0.;
        for (int i = 0; i < score.length; i++) {
            sum += score[i];
            if (maxProb < score[i]) {
                maxIdx = i;
                maxProb = score[i];
            }
        }
        if (predDetail) {
            for (int i = 0; i < score.length; i++) {
                this.details.replace(labels.get(i), score[i] / sum);
            }
        }

        Object label = labels.get(maxIdx);
        output.setField(0, label);
        if (predDetail) {
            output.setField(1, gson.toJson(details));
        }
        return outputColsHelper.getResultRow(row, output);
    }
}
