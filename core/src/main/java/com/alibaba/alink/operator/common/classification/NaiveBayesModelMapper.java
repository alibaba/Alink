package com.alibaba.alink.operator.common.classification;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.classification.NaiveBayesPredictParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This mapper predicts sample label.
 */
public class NaiveBayesModelMapper extends RichModelMapper {
    public String[] colNames;
    public String vectorColName;
    private int vectorIndex;
    private int[] featIdx;
    private NaiveBayesPredictModelData modelData;

    /**
     * Construct function.
     *
     * @param modelSchema serializer schema.
     * @param dataSchema  data schema.
     * @param params      parameters for predict.
     */
    public NaiveBayesModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);
        this.colNames = dataSchema.getFieldNames();
        this.vectorColName = params.get(NaiveBayesPredictParams.VECTOR_COL);

        if (this.vectorColName != null && this.vectorColName.length() != 0) {
            this.vectorIndex = TableUtil.findColIndex(this.colNames, this.vectorColName);
        }
    }

    /**
     * Calculate the probability that the input data belongs to each class in multinomial method.
     *
     * @param vec input data.
     * @return the probability that the input data belongs to each class.
     */
    private double[] multinomialCalculation(Vector vec) {
        int rowSize = modelData.theta.numRows();
        DenseVector prob = DenseVector.zeros(rowSize);
        DenseVector pi = new DenseVector(modelData.pi);
        if (vec instanceof DenseVector) {
            BLAS.gemv(1, modelData.theta, false, (DenseVector)vec, 0, prob);
        } else {
            BLAS.gemv(1, modelData.theta, false, (SparseVector)vec, 0, prob);
        }
        BLAS.axpy(1, pi, prob);
        return prob.getData();
    }

    /**
     * Calculate the probability that the input data belongs to each class in bernoulli method.
     *
     * @param vec input data.
     * @return the probability that the input data belongs to each class.
     */
    private double[] bernoulliCalculation(Vector vec) {
        int rowSize = modelData.theta.numRows();
        int colSize = modelData.theta.numCols();
        DenseVector prob = DenseVector.zeros(rowSize);
        DenseVector pi = new DenseVector(modelData.pi);
        DenseVector phi = new DenseVector(modelData.phi);
        if (vec instanceof DenseVector) {
            DenseVector denseVec = (DenseVector)vec;
            for (int j = 0; j < colSize; ++j) {
                double value = denseVec.get(j);
                Preconditions.checkArgument(value == 0. || value == 1.,
                    "Bernoulli naive Bayes requires 0 or 1 feature values.");
            }
            BLAS.gemv(1, modelData.minMat, false, denseVec, 0, prob);
        } else {
            BLAS.gemv(1, modelData.minMat, false, (SparseVector)vec, 0, prob);
        }
        BLAS.axpy(1, pi, prob);
        BLAS.axpy(1, phi, prob);
        return prob.getData();
    }

    @Override
    public void loadModel(List<Row> modelRows) {
        modelData = new NaiveBayesModelDataConverter().load(modelRows);
        int size;
        if (modelData.featureNames != null) {
            size = modelData.featureNames.length;
            featIdx = new int[size];
            for (int i = 0; i < size; ++i) {
                featIdx[i] = TableUtil.findColIndex(colNames, modelData.featureNames[i]);
            }
        } else {
            featIdx = new int[1];
            featIdx[0] = TableUtil.findColIndex(colNames, vectorColName);
        }
    }

    /**
     * Calculate the probability of each label and return the most possible one.
     *
     * @param row the input data.
     * @return the most possible label.
     */
    @Override
    protected Object predictResult(Row row) {
        double[] prob = calculateProb(row);
        return findMaxProbLabel(prob, modelData.label);
    }

    /**
     * Calculate the probability of each label and return the most possible one and the detail.
     *
     * @param row the input data.
     * @return the most possible label and the detail.
     */
    @Override
    protected Tuple2<Object, String> predictResultDetail(Row row) {
        double[] prob = calculateProb(row);
        double maxProb = prob[0];
        for (int i = 1; i < prob.length; ++i) {
            if (maxProb < prob[i]) {
                maxProb = prob[i];
            }
        }
        double sumProb = 0.0;
        for (double probVal : prob) {
            sumProb += Math.exp(probVal - maxProb);
        }
        sumProb = maxProb + Math.log(sumProb);
        for (int i = 0; i < prob.length; ++i) {
            prob[i] = Math.exp(prob[i] - sumProb);
        }

        Object result = findMaxProbLabel(prob, modelData.label);

        int labelSize = modelData.pi.length;
        Map<String, Double> detail = new HashMap<>(labelSize);
        for (int i = 0; i < labelSize; ++i) {
            detail.put(modelData.label[i].toString(), prob[i]);
        }
        String jsonDetail = JsonConverter.toJson(detail);
        return new Tuple2<>(result, jsonDetail);
    }

    private static Object findMaxProbLabel(double[] prob, Object[] label) {
        Object result = null;
        int probSize = prob.length;
        double maxVal = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < probSize; ++i) {
            if (maxVal < prob[i]) {
                maxVal = prob[i];
                result = label[i];
            }
        }
        return result;
    }

    private double[] calculateProb(Row row) {
        Vector featVec;
        if (vectorColName != null) {
            featVec = VectorUtil.getVector(row.getField(this.vectorIndex));
        } else {
            double[] vals = new double[modelData.featLen];
            for (int i = 0; i < modelData.featLen; ++i) {
                Object data = row.getField(featIdx[i]);
                vals[i] = data instanceof Number ?
                    ((Number)data).doubleValue() :
                    Double.parseDouble(data.toString());
            }
            featVec = new DenseVector(vals);
        }
        if (NaiveBayesModelDataConverter.BayesType.MULTINOMIAL.equals(modelData.modelType)) {
            return multinomialCalculation(featVec);
        } else {
            return bernoulliCalculation(featVec);
        }
    }
}
