package com.alibaba.alink.operator.common.fm;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.FmDataFormat;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

/**
 * Model info of FM.
 */
public class FmModelInfo {

    private int[] dim;
    private String task;
    private int vectorSize;
    private FmDataFormat factors;
    private int[] filedPos;
    private String[] colNames;

    public int[] getDim() {
        return dim;
    }

    public String getTask() {
        return task;
    }

    public int getVectorSize() {
        return vectorSize;
    }

    public double[][] getFactors() {
        return factors.factors;
    }

    public int[] getFiledPos() {
        return filedPos;
    }

    public String[] getColNames() {
        return colNames;
    }

    public FmModelInfo(List<Row> rows, TypeInformation labelType) {
        FmModelData modelData = new FmModelDataConverter(labelType).load(rows);
        this.dim = modelData.dim;
        this.task = modelData.task.toString();
        this.vectorSize = modelData.vectorSize;
        this.colNames = modelData.featureColNames;
        this.filedPos = modelData.fieldPos;
        this.factors = modelData.fmModel;
    }

    @Override
    public String toString() {
        StringBuilder sbd = new StringBuilder();
        DecimalFormat df = new DecimalFormat("#0.00000000");

        sbd.append(PrettyDisplayUtils.displayHeadline("meta info", '-'));
        Map<String, String> map = new HashMap<>();
        map.put("vectorSize", String.valueOf(vectorSize));
        map.put("task", task);
        map.put("dim", JsonConverter.toJson(dim));
        if (filedPos != null) {
            map.put("filedPos", JsonConverter.toJson(filedPos));
        }
        double bias = factors.bias;
        map.put("bias", String.valueOf(bias));
        sbd.append(PrettyDisplayUtils.displayMap(map, 3, true) + "\n");

        sbd.append(PrettyDisplayUtils.displayHeadline("model info", '-'));
        int k = dim[2];
        if (colNames != null) {
            int printSize = colNames.length < 10 ? colNames.length : 10;
            Object[][] out = new Object[printSize + 1][3];

            for (int i = 0; i < printSize; ++i) {
                out[i][0] = colNames[i];
                if (dim[1] > 0) {
                    out[i][1] = df.format(factors.linearItems[i]);
                } else {
                    out[i][1] = df.format(0.0);
                }
                String factor = "";

                for (int j = 0; j < k; ++j) {
                    factor += df.format(factors.factors[i][j]) + " ";
                }
                out[i][2] = factor;
            }
            if (colNames.length >= 10) {
                for (int i = 0; i < 3; ++i) {
                    out[printSize - 1][i] = "... ...";
                }
            }

            sbd.append(PrettyDisplayUtils.displayTable(out, printSize, 3, null,
                new String[] {"colName", "linearItem", "factor"}, null,
                printSize, 3));
        } else {
            int printSize = vectorSize < 10 ? vectorSize : 10;
            Object[][] out = new Object[printSize + 1][3];

            for (int i = 0; i < printSize; ++i) {
                out[i][0] = String.valueOf(i);
                if (dim[1] > 0) {
                    out[i][1] = df.format(factors.linearItems[i]);
                } else {
                    out[i][1] = df.format(0.0);
                }
                String factor = "";

                for (int j = 0; j < k; ++j) {
                    factor += df.format(factors.factors[i][j]) + " ";
                }
                out[i][2] = factor;
            }
            if (vectorSize >= 10) {
                for (int i = 0; i < 3; ++i) {
                    out[printSize - 1][i] = "... ...";
                }
            }

            sbd.append(PrettyDisplayUtils.displayTable(out, printSize, 3, null,
                new String[] {"colName", "linearItem", "factor"}, null,
                printSize, 3));
        }

        return sbd.toString();
    }
}
