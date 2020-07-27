package com.alibaba.alink.operator.common.linear;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;

import org.apache.flink.types.Row;

/**
 * Linear Regressor (lasso, ridge, linearReg) model info.
 */
public class LinearRegressorModelInfo implements Serializable {
    private static final long serialVersionUID = 1587799722352066332L;
    private String[] featureNames;
    private String vectorColName;
    private DenseVector coefVector;
    private int vectorSize;
    private String modelName;
    private boolean hasInterceptItem = true;
    private static final int WIDTH = 10;
    protected Object[] labelValues;
    public String[] getFeatureNames() {
        return featureNames;
    }

    public String getVectorColName() {
        return vectorColName;
    }

    public DenseVector getWeight() {
        return coefVector;
    }

    public int getVectorSize() {
        return vectorSize;
    }

    public String getModelName() {
        return modelName;
    }

    public boolean hasInterceptItem() {
        return hasInterceptItem;
    }

    public LinearRegressorModelInfo(List<Row> rows) {
        LinearModelData modelData = new LinearModelDataConverter().load(rows);
        featureNames = modelData.featureNames;
        vectorColName = modelData.vectorColName;
        coefVector = modelData.coefVector;
        vectorSize = modelData.vectorSize;
        modelName = modelData.modelName;
        hasInterceptItem = modelData.hasInterceptItem;
        processLabelValues(modelData);
    }

    protected void processLabelValues(LinearModelData modelData) {
    }

    @Override
    public String toString() {
        DecimalFormat df = new DecimalFormat("#0.00000000");
        StringBuilder ret = new StringBuilder();
        Map<String, String> map = new HashMap<>();
        map.put("model name", modelName);
        map.put("num feature", String.valueOf(vectorSize));
        if (vectorColName != null) {
            map.put("vector colName", vectorColName);
        }
        map.put("hasInterception", String.valueOf(hasInterceptItem));
        ret.append(PrettyDisplayUtils.displayHeadline("model meta info", '-'));
        ret.append(PrettyDisplayUtils.displayMap(map, WIDTH, false) + "\n");

        if (labelValues != null && labelValues.length > 1) {
            ret.append(PrettyDisplayUtils.displayHeadline("model label values", '-'));
            ret.append(PrettyDisplayUtils.displayList(java.util.Arrays.asList(labelValues)) + "\n");
        }

        ret.append(PrettyDisplayUtils.displayHeadline("model weight info", '-'));
        if (coefVector.size() < WIDTH) {
            Object[][] out = new Object[1][coefVector.size()];
            String[] tableColNames = new String[coefVector.size()];
            int startIdx = 0;
            if (hasInterceptItem) {
                startIdx = 1;
                tableColNames[0] = "intercept";
                out[0][0] = coefVector.get(0);
            }
            for (int i = startIdx; i < coefVector.size(); ++i) {
                tableColNames[i] = featureNames != null ? featureNames[i - startIdx] : String.valueOf(i);
                out[0][i] = df.format(coefVector.get(i));
            }
            ret.append(PrettyDisplayUtils.displayTable(out, 1, coefVector.size(),
                null, tableColNames, null, 1, coefVector.size()));
        } else {
            Object[][] out = new Object[1][WIDTH];
            String[] tableColNames = new String[WIDTH];
            int startIdx = 0;
            if (hasInterceptItem) {
                startIdx = 1;
                tableColNames[0] = "intercept";
                out[0][0] = coefVector.get(0);
            }
            for (int i = startIdx; i < WIDTH - 1; ++i) {
                tableColNames[i] = featureNames != null ? featureNames[i - startIdx] : String.valueOf(i);
                out[0][i] = df.format(coefVector.get(i));
            }
            tableColNames[WIDTH - 1] = "... ...";
            out[0][WIDTH - 1] = "... ...";
            ret.append(PrettyDisplayUtils.displayTable(out, 1, WIDTH,
                null, tableColNames, null, 1, WIDTH));
        }
        return ret.toString();

    }
}
