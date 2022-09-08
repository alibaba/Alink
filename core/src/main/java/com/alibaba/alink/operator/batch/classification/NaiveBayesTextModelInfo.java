package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NaiveBayesTextModelInfo implements Serializable {
    private static final long serialVersionUID = -233174684705817660L;
    private DenseMatrix theta;
    private double[] piArray;
    private Object[] labels;
    private int vectorSize;
    public String vectorColName;
    public String modelType;

    public NaiveBayesTextModelInfo() {
    }

    public NaiveBayesTextModelInfo(DenseMatrix theta,
								   double[] piArray,
								   Object[] labels,
								   int vectorSize,
								   String vectorColName,
								   String modelType) {
        this.theta = theta;
        this.piArray = piArray;
        this.labels = labels;
        this.vectorSize = vectorSize;
        this.vectorColName = vectorColName;
        this.modelType = modelType;
    }

    public NaiveBayesTextModelInfo(List <Row> rows) {
        NaiveBayesTextModelInfo modelInfo = JsonConverter.fromJson((String) rows.get(0).getField(0),
            NaiveBayesTextModelInfo.class);
        this.theta = modelInfo.theta;
        this.piArray = modelInfo.piArray;
        this.labels = modelInfo.labels;
        this.vectorSize = modelInfo.vectorSize;
        this.vectorColName = modelInfo.vectorColName;
        this.modelType = modelInfo.modelType;
    }

    public String getVectorColName() {
        return vectorColName;
    }

    public String getModelType() {
        return modelType;
    }

    public Object[] getLabelList() {
        return labels;
    }

    public double[] getPriorProbability() {
        int piArraySize = piArray.length;
        double[] priorProb = new double[piArraySize];
        for (int i = 0; i < piArraySize; i++) {
            priorProb[i] = Math.exp(piArray[i]);
        }
        return priorProb;
    }

    public DenseMatrix getFeatureProbability() {
        int rowSize = theta.numRows();
        int colSize = theta.numCols();
        DenseMatrix maxLikelihood = new DenseMatrix(rowSize, colSize);
        for (int col = 0; col < colSize; col++) {
            for (int row = 0; row < rowSize; row++) {
                maxLikelihood.set(row, col, Math.exp(theta.get(row, col)));
            }
        }

        return maxLikelihood.transpose();
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();
        res.append(PrettyDisplayUtils.displayHeadline("NaiveBayesTextModelInfo", '-') + "\n");

        Map <String, String> map = new HashMap <>();
        map.put("vector col name", vectorColName);
        map.put("vector size", String.valueOf(vectorSize));
        map.put("model type", modelType);
        res.append(PrettyDisplayUtils.displayHeadline("model meta info", '='));
        res.append(PrettyDisplayUtils.displayMap(map, 10, false) + "\n");

        res.append(PrettyDisplayUtils.displayHeadline("label proportion information", '=') + "\n");
        Tuple3 <String[], Double[][], Integer> labelProportionTable = generateLabelProportionTable(
            getPriorProbability());
        String labelProportions = PrettyDisplayUtils.displayTable(
            labelProportionTable.f1, 1, labelProportionTable.f2,
            null, labelProportionTable.f0, null, 3, 3);
        res.append(labelProportions + "\n");

        res.append(PrettyDisplayUtils.displayHeadline("feature probability information", '=') + "\n");
        Tuple5 <String[], String[], Double[][], Integer, Integer> featureProportionTable
            = generateFeatureProportionTable();
        String featureProportions = PrettyDisplayUtils.displayTable(
            featureProportionTable.f2, featureProportionTable.f3, featureProportionTable.f4,
            featureProportionTable.f0, featureProportionTable.f1, "vector index", 3, 3);
        res.append(featureProportions + "\n");

        return res.toString();
    }

    public Tuple3 <String[], Double[][], Integer> generateLabelProportionTable(double[] priorProb) {
        int length = priorProb.length;
        String[] labels = new String[length];
        Double[] probabilities = new Double[length];
        for (int i = 0; i < priorProb.length; i++) {
            labels[i] = this.labels[i].toString();
            probabilities[i] = priorProb[i];
        }
        return Tuple3.of(labels, new Double[][] {probabilities}, length);
    }

    private Tuple5 <String[], String[], Double[][], Integer, Integer>
    generateFeatureProportionTable() {
        int numLabel = theta.numRows();//feature size
        int numFeature = theta.numCols();//how much label

        String[] rowName = new String[numLabel];
        String[] colName = new String[numFeature];
        for (int i = 0; i < labels.length; i++) {
            rowName[i] = labels[i].toString();
        }
        Double[][] data = new Double[numFeature][numLabel];
        DenseMatrix matrix = getFeatureProbability();
        if ("Bernoulli".equals(modelType)) {
            for (int j = 0; j < numLabel; j++) {
                double sum = 0;
                for (int i = 0; i < numFeature; i++) {
                    sum += matrix.get(i, j);
                    data[i][j] = matrix.get(i, j);
                }
                for (int i = 0; i < numFeature; i++) {
                    data[i][j] /= sum;
                }
            }
            for (int i = 0; i < numFeature; i++) {
                colName[i] = String.valueOf(i);
            }
        } else {
            for (int i = 0; i < numFeature; i++) {
                colName[i] = String.valueOf(i);
                for (int j = 0; j < numLabel; j++) {
                    data[i][j] = matrix.get(i, j);
                }
            }
        }
        return Tuple5.of(colName, rowName, data, numFeature, numLabel);
    }
}
