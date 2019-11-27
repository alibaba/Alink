package com.alibaba.alink.operator.common.regression;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.common.regression.glm.FamilyLink;
import com.alibaba.alink.operator.common.regression.glm.GlmUtil;
import com.alibaba.alink.common.utils.RowUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.regression.GlmPredictParams;
import com.alibaba.alink.params.regression.GlmTrainParams;
import org.apache.commons.lang3.ArrayUtils;

import java.util.List;

/**
 * GLM model mapper for glm predict.
 */
public class GlmModelMapper extends ModelMapper {

    /**
     * coefficients of each features.
     */
    private double[] coefficients;

    /**
     * intercept.
     */
    private double intercept;

    /**
     * family and link function.
     */
    private FamilyLink familyLink;

    /**
     * offset col idx.
     */
    private int offsetColIdx;

    /**
     * feature col indices.
     */
    private int[] featureColIdxs;

    /**
     * if need calculate link predict col.
     */
    private boolean hasLinkPredit;

    /**
     * features.
     */
    private double[] features;

    /**
     *
     * @param modelSchema
     * @param dataSchema
     * @param params
     */
    public GlmModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);
        String linkPredResultColName = params.get(GlmPredictParams.LINK_PRED_RESULT_COL);

        hasLinkPredit = true;
        if (linkPredResultColName == null || linkPredResultColName.isEmpty()) {
            hasLinkPredit = false;
        }
    }

    /**
     *
     * @param modelRows the list of Row type data
     */
    @Override
    public void loadModel(List<Row> modelRows) {
        GlmModelDataConverter model = new GlmModelDataConverter();
        GlmModelData modelData = model.load(modelRows);

        coefficients = modelData.coefficients;
        intercept = modelData.intercept;
        TableSchema dataSchema = getDataSchema();
        if (modelData.offsetColName == null || modelData.offsetColName.isEmpty()) {
            offsetColIdx = -1;
        } else {
            offsetColIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), modelData.offsetColName);
        }

        featureColIdxs = new int[modelData.featureColNames.length];
        for (int i = 0; i < featureColIdxs.length; i++) {
            featureColIdxs[i] = TableUtil.findColIndex(dataSchema.getFieldNames(), modelData.featureColNames[i]);
            if (featureColIdxs[i] < 0) {
                throw new RuntimeException("offset col not exist." + modelData.featureColNames[i]);
            }
        }

        features = new double[featureColIdxs.length];

        String familyName = params.get(GlmTrainParams.FAMILY);
        double variancePower = params.get(GlmTrainParams.VARIANCE_POWER);
        String linkName = params.get(GlmTrainParams.LINK);
        double linkPower = params.get(GlmTrainParams.LINK_POWER);

        familyLink = new FamilyLink(familyName, variancePower, linkName, linkPower);

    }

    /**
     *
     * @return table schema.
     */
    @Override
    public TableSchema getOutputSchema() {
        String predResultColName = params.get(GlmPredictParams.PREDICTION_COL);
        String linkPredResultColName = params.get(GlmPredictParams.LINK_PRED_RESULT_COL);

        String[] colNames;
        TypeInformation[] colTypes;

        TableSchema dataSchema = getDataSchema();
        if (linkPredResultColName == null || linkPredResultColName.isEmpty()) {
            colNames = ArrayUtils.add(dataSchema.getFieldNames(), predResultColName);
            colTypes = ArrayUtils.add(dataSchema.getFieldTypes(), Types.DOUBLE());
        } else {
            colNames = ArrayUtils.addAll(dataSchema.getFieldNames(),
                new String[]{predResultColName, linkPredResultColName});
            colTypes = ArrayUtils.addAll(dataSchema.getFieldTypes(),
                new TypeInformation[]{Types.DOUBLE(), Types.DOUBLE()});
        }
        return new TableSchema(colNames, colTypes);
    }

    /**
     *
     * @param row the input Row type data
     * @return
     */
    @Override
    public Row map(Row row) {
        if (row == null) {
            return row;
        }

        double offset = 0;
        if (offsetColIdx >= 0) {
            offset = (Double) row.getField(offsetColIdx);
        }

        for (int i = 0; i < features.length; i++) {
            features[i] = (Double) row.getField(featureColIdxs[i]);
        }

        double predict = GlmUtil.predict(coefficients, intercept, features, offset, familyLink);

        if (hasLinkPredit) {
            double eta = GlmUtil.linearPredict(coefficients, intercept, features) + offset;
            return RowUtil.merge(row, Row.of(predict, eta));
        } else {
            return RowUtil.merge(row, Row.of(predict));
        }
    }
}
