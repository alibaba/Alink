package com.alibaba.alink.operator.batch.classification;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.classification.NaiveBayesModelDataConverter;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelData;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelDataConverter;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.*;

import static com.alibaba.alink.operator.common.utils.PrettyDisplayUtils.displayList;

public class NaiveBayesModelInfo implements Serializable {
    private String[] featureNames;
    private int featureSize;
    private boolean[] isCategorical;
    private double[] labelWeights;
    private Object[] labels;
    private int labelSize;
    public double[][] weightSum;
    public SparseVector[][] featureInfo;
    public transient List<Row> stringIndexerModelSerialized;
    private HashMap<Object, HashSet<Object>> cateFeatureValue;

    public NaiveBayesModelInfo() {
    }

    public NaiveBayesModelInfo(String[] featureNames,
                               boolean[] isCategorical,
                               double[] labelWeights,
                               Object[] labels,
                               double[][] weightSum,
                               SparseVector[][] featureInfo,
                               List<Row> stringIndexerModelSerialized) {
        this.featureNames = featureNames;
        featureSize = featureNames.length;
        cateFeatureValue = new HashMap<>(featureSize);
        this.isCategorical = isCategorical;
        this.labelWeights = labelWeights;
        this.labels = labels;
        labelSize = labels.length;
        this.weightSum = weightSum;
        this.featureInfo = featureInfo;
        this.stringIndexerModelSerialized = stringIndexerModelSerialized;
    }

    public NaiveBayesModelInfo(List<Row> rows) {
        NaiveBayesModelInfo modelData = new NaiveBayesModelDataConverter().loadModelInfo(rows);
        featureNames = modelData.featureNames;
        featureSize = featureNames.length;
        cateFeatureValue = new HashMap<>(featureSize);
        isCategorical = modelData.isCategorical;
        labelWeights = modelData.labelWeights;
        labels = modelData.labels;
        labelSize = labels.length;
        weightSum = modelData.weightSum;
        featureInfo = modelData.featureInfo;
        stringIndexerModelSerialized = modelData.stringIndexerModelSerialized;
    }

    public String[] getFeatureNames() {
        return featureNames;
    }

    /**
     * This function gets the feature information of categorical features.
     * For each categorical feature, this function calculates the proportion among all the labels.
     */
    public HashMap<Object, HashMap<Object, HashMap<Object, Double>>> getCategoryFeatureInfo() {
        MultiStringIndexerModelData model = new MultiStringIndexerModelDataConverter()
            .load(stringIndexerModelSerialized);
        HashMap<Object, HashMap<Object, HashMap<Object, Double>>> labelFeatureMap = new HashMap<>(labelSize);
        String[] cateCols = model.meta.get(HasSelectedCols.SELECTED_COLS);
        int tokenNumber = cateCols.length;
        HashMap<Long, String>[] tokenIndex = new HashMap[tokenNumber];
        for (int i = 0; i < tokenNumber; i++) {
            tokenIndex[i] = new HashMap<>((int) model.getNumberOfTokensOfColumn(cateCols[i]));
        }
        for (Tuple3<Integer, String, Long> tuple3 : model.tokenAndIndex) {
            tokenIndex[tuple3.f0].put(tuple3.f2, tuple3.f1);
        }

        int cateIndex = 0;
        for (int i = 0; i < featureSize; i++) {
            if (isCategorical[i]) {
                String featureName = featureNames[i];
                HashSet<Object> featureValue = new HashSet<>();

                double[] featureSum = new double[Math.toIntExact(model.getNumberOfTokensOfColumn(cateCols[cateIndex]))];

                for (int j = 0; j < labelSize; j++) {
                    SparseVector sv = featureInfo[j][i];
                    int[] svIndices = sv.getIndices();
                    double[] svValues = sv.getValues();
                    int feaValNum = svIndices.length;//the value number of this feature.
                    for (int k = 0; k < feaValNum; k++) {
                        featureSum[svIndices[k]] += svValues[k];
                    }
                }

                for (int j = 0; j < labelSize; j++) {
                    SparseVector sv = featureInfo[j][i];
                    int[] svIndices = sv.getIndices();
                    double[] svValues = sv.getValues();
                    int feaValNum = svIndices.length;
                    HashMap<Object, HashMap<Object, Double>> v;
                    if (!labelFeatureMap.containsKey(labels[j])) {
                        v = new HashMap<>();
                    } else {
                        v = labelFeatureMap.get(labels[j]);
                    }
                    HashMap<Object, Double> featureValues = new HashMap<>();
                    for (int k = 0; k < feaValNum; k++) {
                        Object key = tokenIndex[cateIndex].get((long) svIndices[k]);
                        featureValue.add(key);
                        double value = svValues[k] / featureSum[svIndices[k]];
                        featureValues.put(key, value);
                    }
                    v.put(featureName, featureValues);
                    labelFeatureMap.put(labels[j], v);
                }
                cateIndex++;
                cateFeatureValue.put(featureName, featureValue);
            }
        }

        //transform
        List<String> listFeature = new ArrayList<>();
        for (int i = 0; i < featureSize; i++) {
            if (isCategorical[i]) {
                listFeature.add(featureNames[i]);
            }
        }
        HashMap<Object, HashMap<Object, HashMap<Object, Double>>> res = new HashMap<>(featureSize);
        for (String o : listFeature) {
            HashMap<Object, HashMap<Object, Double>> labelMap = new HashMap<>(labelSize);
            for (Object label : labels) {
                labelMap.put(label, labelFeatureMap.get(label).get(o));
            }
            res.put(o, labelMap);
        }
        return res;
    }

    /**
     * This function gets the feature information of continuous features.
     * For each continuous feature, this function calculates the mean and sigma under each the labels.
     */
    public HashMap<Object, double[][]> getGaussFeatureInfo() {
        HashMap<Object, double[][]> res = new HashMap<>(labelSize);
        for (int i = 0; i < featureSize; i++) {
            if (!isCategorical[i]) {
                for (int j = 0; j < labelSize; j++) {
                    double[][] v;
                    if (!res.containsKey(labels[j])) {
                        v = new double[featureSize][];
                    } else {
                        v = res.get(labels[j]);
                    }
                    v[i] = featureInfo[j][i].getValues();
                    res.put(labels[j], v);
                }
            }
        }
        return res;
    }

    public Object[] getLabelList() {
        return labels;
    }

    public Map<Comparable, Double> getLabelProportion() {
        normalizeArray(labelWeights);
        Map<Comparable, Double> labelProportion = new HashMap<>(labels.length);
        for (int i = 0; i < labels.length; i++) {
            labelProportion.put((Comparable) labels[i], labelWeights[i]);
        }
        return labelProportion;
    }


    public Map<String, Boolean> getCategoryInfo() {

        Map<String, Boolean> categoryInfo = new HashMap<>(featureNames.length);
        for (int i = 0; i < featureNames.length; i++) {
            categoryInfo.put(featureNames[i], isCategorical[i]);
        }
        return categoryInfo;
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();
        res.append(PrettyDisplayUtils.displayHeadline("NaiveBayesTextModelInfo", '-') + "\n");

        Map<String, String> map = new HashMap<>();
        map.put("feature col names", JsonConverter.toJson(featureNames));
        map.put("feature size", String.valueOf(featureSize));
        map.put("labels", JsonConverter.toJson(labels));
        map.put("label number", String.valueOf(labelSize));
        res.append(PrettyDisplayUtils.displayHeadline("model meta info", '='));
        res.append(PrettyDisplayUtils.displayMap(map, 10, false) + "\n");

        Map<Comparable, Double> labelProportion = getLabelProportion();
        List<Double> proportionInfo = Arrays.asList(labelProportion.values().toArray(new Double[0]));
        List<Comparable> labelInfo = Arrays.asList(labelProportion.keySet().toArray(new Comparable[0]));
        res.append(PrettyDisplayUtils.displayHeadline("label proportion information", '=') + "\n");
        res.append("label info:");
        res.append(displayList(labelInfo, false) + "\n");
        res.append("proportion:");
        res.append(displayList(proportionInfo, false) + "\n");

        Map<String, Boolean> categoryInfo = getCategoryInfo();
        List<String> cate = new ArrayList<>();
        List<String> gauss = new ArrayList<>();
        for (Map.Entry<String, Boolean> entry : categoryInfo.entrySet()) {
            if (entry.getValue()) {
                cate.add(entry.getKey());
            } else {
                gauss.add(entry.getKey());
            }
        }
        res.append(PrettyDisplayUtils.displayHeadline("category information", '=') + "\n");
        res.append("categorical features: ");
        res.append(displayList(cate, false) + "\n");
        res.append("gaussian features: ");
        res.append(displayList(gauss, false) + "\n");

        res.append(PrettyDisplayUtils.displayHeadline("categorical features proportion information", '=') + "\n");
        this.printCateFeatureInfo(res);

        res.append(PrettyDisplayUtils.displayHeadline("continuous features mean sigma information", '=') + "\n");
        this.printMeanSigma(res);

        return res.toString();
    }

    private static void normalizeArray(double[] labelWeights) {
        double sum = 0;
        for (double val : labelWeights) {
            sum += val;
        }
        for (int i = 0; i < labelWeights.length; i++) {
            labelWeights[i] /= sum;
        }
    }


    /**
     * This function helps print categorical feature information.
     */
    private void printCateFeatureInfo(StringBuilder sb) {
        HashMap<Object, HashMap<Object, HashMap<Object, Double>>> cateFeaInfo = getCategoryFeatureInfo();
        for (Map.Entry<Object, HashMap<Object, HashMap<Object, Double>>> featureEntry : cateFeaInfo.entrySet()) {
            Object featureName = featureEntry.getKey();
            Object[] featureValues = cateFeatureValue.get(featureName).toArray();
            int valueNumber = featureValues.length;
            String[] colName = new String[valueNumber];
            for (int i = 0; i < valueNumber; i++) {
                colName[i] = featureValues[i].toString();
            }
            String[] rowName = new String[labelSize];
            for (int i = 0; i < labelSize; i++) {
                rowName[i] = labels[i].toString();
            }
            Double[][] data = new Double[labelSize][valueNumber];
            for (int i = 0; i < labelSize; i++) {
                Arrays.fill(data[i], 0.);
                Object label = labels[i];
                HashMap<Object, Double> labelValue = featureEntry.getValue().get(label);
                for (int j = 0; j < valueNumber; j++) {
                    Object featureValue = featureValues[j];
                    if (labelValue.containsKey(featureValue)) {
                        data[i][j] = labelValue.get(featureValue);
                    }
                }
            }
            sb.append("The features proportion information of " + featureName.toString() + ":\n");
            String featureProportions = PrettyDisplayUtils.displayTable(
                data, labelSize, valueNumber,
                rowName, colName, null, 3, 3);
            sb.append(featureProportions + "\n");
        }
    }

    /**
     * This function helps print continuous feature information.
     */
    private void printMeanSigma(StringBuilder sb) {
        HashMap<Object, double[][]> meanSigma = getGaussFeatureInfo();
        int gaussColNum = 0;
        for (boolean b : isCategorical) {
            if (!b) {
                gaussColNum++;
            }
        }
        String[] rowName = new String[labelSize];
        for (int i = 0; i < labelSize; i++) {
            rowName[i] = labels[i].toString();
        }
        String[] colName = new String[featureSize];
        for (int i = 0; i < featureSize; i++) {
            colName[i] = featureNames[i].toString();
        }
        Double[][] meanData = new Double[labelSize][gaussColNum];
        Double[][] stdData = new Double[labelSize][gaussColNum];
        for (int i = 0; i < labelSize; i++) {
            Object label = labels[i];
            int gaussIndex = 0;
            for (int j = 0; j < featureSize; j++) {
                if (!isCategorical[j]) {
                    double[] datum = meanSigma.get(label)[j];
                    meanData[i][gaussIndex] = datum[0];
                    stdData[i][gaussIndex] = datum[1];
                    ++gaussIndex;
                }

            }
        }
        sb.append("Mean of features of each label:\n");
        String featureProportions = PrettyDisplayUtils.displayTable(
            meanData, labelSize, gaussColNum,
            rowName, colName, null, 3, 3);
        sb.append(featureProportions + "\n");
        sb.append("Std of features of each label:\n");
        featureProportions = PrettyDisplayUtils.displayTable(
            stdData, labelSize, gaussColNum,
            rowName, colName, null, 3, 3);
        sb.append(featureProportions + "\n");
    }
}
