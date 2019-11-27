package com.alibaba.alink.operator.common.regression;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.AftRegObjFunc;
import com.alibaba.alink.operator.common.linear.FeatureLabelUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.classification.LinearModelMapperParams;
import com.alibaba.alink.params.regression.AftRegPredictParams;

import java.util.List;

/**
 * Accelerated Failure Time Survival Regression.
 * Based on the Weibull distribution of the survival time.
 * <p>
 * (https://en.wikipedia.org/wiki/Accelerated_failure_time_model)
 */
public class AFTModelMapper extends RichModelMapper {

    private int vectorColIndex = -1;
    private double[] quantileProbabilities;
    private LinearModelData model;
    private int[] featureIdx;
    private int featureN;

    /**
     * Constructor.
     *
     * @param modelSchema the model schema.
     * @param dataSchema  the data schema.
     * @param params      the params.
     */
    public AFTModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);
        this.quantileProbabilities = params.get(AftRegPredictParams.QUANTILE_PROBABILITIES);
        if (null != params) {
            String vectorColName = params.get(LinearModelMapperParams.VECTOR_COL);
            if (null != vectorColName && vectorColName.length() != 0) {
                this.vectorColIndex = TableUtil.findColIndex(dataSchema.getFieldNames(), vectorColName);
            }
        }
    }

    /**
     * Load model from the row type data.
     *
     * @param modelRows the list of Row type data
     */
    @Override
    public void loadModel(List<Row> modelRows) {
        this.model = new LinearModelDataConverter(LinearModelDataConverter.extractLabelType(super.getModelSchema()))
                .load(modelRows);
        if (vectorColIndex == -1) {
            TableSchema dataSchema = getDataSchema();
            if (this.model.featureNames != null) {
                this.featureN = this.model.featureNames.length;
                this.featureIdx = new int[this.featureN];
                String[] predictTableColNames = dataSchema.getFieldNames();
                for (int i = 0; i < this.featureN; i++) {
                    this.featureIdx[i] = TableUtil.findColIndex(predictTableColNames,
                            this.model.featureNames[i]);
                }
            } else {
                vectorColIndex = TableUtil.findColIndex(dataSchema.getFieldNames(), model.vectorColName);
            }
        }
    }

    /**
     * Predict the result.
     *
     * @param row the predict data.
     * @return the predict result.
     */
    @Override
    protected Object predictResult(Row row) {
        Vector aVector = FeatureLabelUtil.getFeatureVector(row, model.hasInterceptItem, this.featureN,
                this.featureIdx, this.vectorColIndex, model.vectorSize);
        double dot = Math.exp(AftRegObjFunc.getDotProduct(aVector, model.coefVector));
        if (dot == Double.POSITIVE_INFINITY) {
            dot = Double.MAX_VALUE;
        }
        return dot;
    }

    /**
     * Predict the result with detailed information.
     *
     * @param row the predict data.
     * @return the predict result with detailed information.
     */
    @Override
    protected Tuple2<Object, String> predictResultDetail(Row row) {
        Vector aVector = FeatureLabelUtil.getFeatureVector(row, model.hasInterceptItem, this.featureN,
                this.featureIdx, this.vectorColIndex, model.vectorSize);
        double[] data = model.coefVector.getData();
        double scale = data[data.length - 1];
        double[] res = new double[quantileProbabilities.length];
        double dot = Math.exp(AftRegObjFunc.getDotProduct(aVector, model.coefVector));
        if (dot == Double.POSITIVE_INFINITY) {
            dot = Double.MAX_VALUE;
        }
        for (int i = 0; i < quantileProbabilities.length; i++) {
            res[i] = dot * Math.exp(Math.log(-Math.log(1 - quantileProbabilities[i])) * scale);
        }
        return Tuple2.of(dot, new DenseVector(res).toString());
    }
}
