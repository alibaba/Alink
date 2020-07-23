package com.alibaba.alink.operator.batch.classification;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.classification.NaiveBayesTextModelDataConverter;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NaiveBayesTextModelInfo implements Serializable {
    private ArrayList<Tuple3<Object, Double, DenseVector>> modelArray;
    private int vectorSize;
    public String vectorColName;
    public String modelType;

    public NaiveBayesTextModelInfo() {}

    NaiveBayesTextModelInfo(ArrayList <Tuple3 <Object, Double, DenseVector>> modelArray,
                            int vectorSize,
                            String vectorColName,
                            String modelType) {
        this.modelArray = modelArray;
        this.vectorSize = vectorSize;
        this.vectorColName = vectorColName;
        this.modelType = modelType;
    }

    public NaiveBayesTextModelInfo(List<Row> rows) {
        NaiveBayesTextModelInfo modelInfo = new NaiveBayesTextModelDataConverter().loadModelInfo(rows);
        this.modelArray = modelInfo.modelArray;
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
        Object[] labels = new Object[modelArray.size()];
        for (int i = 0; i < labels.length; i++) {
            labels[i] = modelArray.get(i).f0;
        }
        return labels;
    }

    public Map<Comparable, Double> getLabelProportion() {
        HashMap<Comparable, Double> labelProportion = new HashMap<>();
        for (Tuple3<Object, Double, DenseVector> tuple3 : modelArray) {
            labelProportion.put((Comparable) tuple3.f0, tuple3.f1);
        }
        normalizeArray(labelProportion);
        return labelProportion;
    }

    public Map<Comparable, Double>[] getFeatureLabelInfo() {
        Map<Comparable, Double>[] featureLabelInfo = new HashMap[vectorSize];
        for (int i = 0; i < vectorSize; i++) {
            featureLabelInfo[i] = new HashMap<>(modelArray.size());
            for (Tuple3<Object, Double, DenseVector> tuple3 : modelArray) {
                int finalI = i;
                featureLabelInfo[i].compute(
                    (Comparable) tuple3.f0, (k, v) -> v == null ? tuple3.f2.get(finalI) : v + tuple3.f2.get(finalI));
            }
        }

        return featureLabelInfo;
    }

    public Map<Comparable, Double>[] getPositiveFeatureProportionPerLabel() {
        Map<Comparable, Double>[] featureProportionInfo = new HashMap[vectorSize];
        for (int i = 0; i < vectorSize; i++) {
            featureProportionInfo[i] = new HashMap<>(modelArray.size());
            for (Tuple3<Object, Double, DenseVector> tuple3 : modelArray) {
                featureProportionInfo[i].put((Comparable) tuple3.f0, tuple3.f2.get(i)/tuple3.f1);
            }
        }

        return featureProportionInfo;
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();
        res.append(PrettyDisplayUtils.displayHeadline("NaiveBayesTextModelInfo", '-')+"\n");

        Map<String, String> map = new HashMap<>();
        map.put("vector col name", vectorColName);
        map.put("vector size", String.valueOf(vectorSize));
        map.put("model type", modelType);
        res.append(PrettyDisplayUtils.displayHeadline("model meta info", '-'));
        res.append(PrettyDisplayUtils.displayMap(map, 10, false) + "\n");

        res.append(PrettyDisplayUtils.displayHeadline("label proportion information", '=')+"\n");
        Tuple3<String[], Double[][], Integer> labelProportionTable = generateLabelProportionTable(getLabelProportion());
        String labelProportions = PrettyDisplayUtils.displayTable(
            labelProportionTable.f1, 1, labelProportionTable.f2,
            null, labelProportionTable.f0, null, 3, 3);
        res.append(labelProportions + "\n");
        res.append(PrettyDisplayUtils.displayHeadline("positive feature proportion information", '=')+"\n");

        Tuple5<String[], String[], Double[][], Integer, Integer> featureLabelProportionTable = generateFeatureLabelTable();
        String featureLabelProportions = PrettyDisplayUtils.displayTable(
            featureLabelProportionTable.f2, featureLabelProportionTable.f3, featureLabelProportionTable.f4,
            featureLabelProportionTable.f0, featureLabelProportionTable.f1, null, 3, 3);
        res.append(featureLabelProportions + "\n");
        res.append(PrettyDisplayUtils.displayHeadline("feature label information", '=')+"\n");
        Tuple5<String[], String[], Double[][], Integer, Integer> featureProportionTable = generateFeatureProportionTable();
        String featureProportions = PrettyDisplayUtils.displayTable(
            featureProportionTable.f2, featureProportionTable.f3, featureProportionTable.f4,
            featureProportionTable.f0, featureProportionTable.f1, null, 3, 3);
        res.append(featureProportions + "\n");

        return res.toString();
    }

    private static void normalizeArray(HashMap<Comparable, Double> labelProportion) {
        double sum = 0;
        for (double val : labelProportion.values()) {
            sum += val;
        }
        double finalSum = sum;
        for (Comparable key : labelProportion.keySet()) {
            labelProportion.compute(key, (k, v) -> v /= finalSum);
        }
    }

    private static Tuple3<String[], Double[][], Integer> generateLabelProportionTable(Map<Comparable, Double> labelProportion) {
        int length = labelProportion.size();
        String[] labels = new String[length];
        Double[] probabilities = new Double[length];
        int count = 0;
        for (Map.Entry<Comparable, Double> entry : labelProportion.entrySet()) {
            labels[count] = entry.getKey().toString();
            probabilities[count] = entry.getValue();
            count++;
        }
        return Tuple3.of(labels, new Double[][] {probabilities}, length);
    }

    private Tuple5<String[], String[], Double[][], Integer, Integer>
    generateFeatureLabelTable() {
        int rowSize = this.modelArray.get(0).f2.size();//feature size
        int colSize = this.modelArray.size();//how much label
        String[] rowName = new String[rowSize];
        String[] colName = new String[colSize];
        int count = 0;
        for (Tuple3<Object, Double, DenseVector> tuple3 : modelArray) {
            colName[count] = tuple3.f0.toString();
            count++;
        }
        Double[][] data = new Double[rowSize][colSize];
        for (int i = 0; i < rowSize; i++) {
            rowName[i] = "f" + i;
            for (int j = 0; j < colSize; j++) {
                data[i][j] = modelArray.get(j).f2.get(i);
            }
        }
        return Tuple5.of(rowName, colName, data, rowSize, colSize);
    }

    private Tuple5<String[], String[], Double[][], Integer, Integer>
    generateFeatureProportionTable() {
        int rowSize = this.modelArray.get(0).f2.size();//feature size
        int colSize = this.modelArray.size();//how much label
        String[] rowName = new String[rowSize];
        String[] colName = new String[colSize];
        int count = 0;
        for (Tuple3<Object, Double, DenseVector> tuple3 : modelArray) {
            colName[count] = tuple3.f0.toString();
            count++;
        }
        Double[][] data = new Double[rowSize][colSize];
        for (int i = 0; i < rowSize; i++) {
            rowName[i] = "f" + i;
            for (int j = 0; j < colSize; j++) {
                data[i][j] = modelArray.get(j).f2.get(i)/modelArray.get(j).f1;
            }
        }
        return Tuple5.of(rowName, colName, data, rowSize, colSize);
    }
}
