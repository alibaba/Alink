package com.alibaba.alink.operator.common.statistics.basicstatistic;

import org.apache.flink.types.Row;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.utils.TableUtil;

import java.util.ArrayList;

/**
 * It is correlation result, which has colNames and correlation values.
 */
public class CorrelationResult {

    /**
     * correlation data.
     */
    DenseMatrix correlation;

    /**
     * If it is vector correlation, colNames is null.
     */
    String[] colNames;

    public CorrelationResult(DenseMatrix correlation) {
        this.correlation = correlation;
    }

    public CorrelationResult(DenseMatrix correlation, String[] colNames) {
        this.correlation = correlation;
        this.colNames = colNames;
    }


    public double[][] getCorrelation() {
        return correlation.getArrayCopy2D();
    }

    public DenseMatrix getCorrelationMatrix() {
        return correlation;
    }


    public String[] getColNames() {
        return colNames;
    }

    @Override
    public String toString() {
        int n = correlation.numRows();
        String[] outColNames = new String[1 + n];
        outColNames[0] = "colName";
        if (colNames != null) {
            System.arraycopy(colNames, 0, outColNames, 1, n);
        } else {
            for (int i = 0; i < n; i++) {
                outColNames[1 + i] = String.valueOf(i);
            }
        }

        ArrayList<Row> data = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Row row = new Row(outColNames.length);
            row.setField(0, outColNames[1 + i]);
            for (int j = 0; j < n; j++) {
                row.setField(1 + j, correlation.get(i, j));
            }
            data.add(row);
        }

        return TableUtil.format(outColNames, data);
    }
}
