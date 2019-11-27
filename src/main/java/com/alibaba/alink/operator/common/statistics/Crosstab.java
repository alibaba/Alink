package com.alibaba.alink.operator.common.statistics;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

/**
 * Cross Tabulations reflects the relationship between two variables.
 */
public class Crosstab {

    /**
     * col names of crosstab.
     */
    public List<String> colTags = new ArrayList<>();

    /**
     * row names of crosstab.
     */
    public List<String> rowTags = new ArrayList<>();

    /**
     * data of crosstab.
     */
    public long[][] data;

    /**
     * convert map to crosstab,
     * eg: 'a', 'b', 2
     * 'a', 'c', 3
     * 'a', 'd', 4
     * after convert:
     * 'b' 'c' 'd'
     * 'a' 2, 3, 4
     * where colTags is ['b','c','d'], rowTags is ['a'],
     * and data is [[2,3,4]]
     */
    public static Crosstab convert(Map<Tuple2<String, String>, Long> maps) {
        Crosstab crosstab = new Crosstab();

        //get row tags and col tags
        Set<Tuple2<String, String>> sets = maps.keySet();

        Set<String> rowTags = new HashSet<>();
        Set<String> colTags = new HashSet<>();
        for (Tuple2<String, String> tuple2 : sets) {
            rowTags.add(tuple2.f0);
            colTags.add(tuple2.f1);
        }

        crosstab.rowTags = new ArrayList<>(rowTags);
        crosstab.colTags = new ArrayList<>(colTags);

        int rowLen = crosstab.rowTags.size();
        int colLen = crosstab.colTags.size();

        //compute value
        crosstab.data = new long[rowLen][colLen];
        for (Map.Entry<Tuple2<String, String>, Long> entry : maps.entrySet()) {
            int rowIdx = crosstab.rowTags.indexOf(entry.getKey().f0);
            int colIdx = crosstab.colTags.indexOf(entry.getKey().f1);
            crosstab.data[rowIdx][colIdx] = entry.getValue();
        }
        return crosstab;
    }

    /**
     *
     * merge left and right crosstab, return new crosstab.
     */
    public static Crosstab merge(Crosstab left, Crosstab right) {
        Crosstab crosstab = new Crosstab();

        Set<String> rowTags = new HashSet<>(left.rowTags);
        rowTags.addAll(right.rowTags);

        Set<String> colTags = new HashSet<>(left.colTags);
        colTags.addAll(right.colTags);

        crosstab.rowTags = new ArrayList<>(rowTags);
        crosstab.colTags = new ArrayList<>(colTags);

        int rowLen = crosstab.rowTags.size();
        int colLen = crosstab.colTags.size();
        crosstab.data = new long[rowLen][colLen];

        //merge data
        int i = 0;
        for (String row : crosstab.rowTags) {
            int j = 0;
            for (String col : crosstab.colTags) {
                long tmp = 0;
                if (left.rowTags.contains(row) && left.colTags.contains(col)) {
                    tmp += left.data[left.rowTags.indexOf(row)][left.colTags.indexOf(col)];
                }
                if (right.rowTags.contains(row) && right.colTags.contains(col)) {
                    tmp += right.data[right.rowTags.indexOf(row)][right.colTags.indexOf(col)];
                }
                crosstab.data[i][j] = tmp;
                j++;
            }
            i++;
        }

        return crosstab;
    }

    /**
     *
     * @return row sum.
     */
    public double[] rowSum() {
        int rowLen = rowTags.size();
        int colLen = colTags.size();

        double[] rowSum = new double[rowLen];

        for (int i = 0; i < rowLen; i++) {
            for (int j = 0; j < colLen; j++) {
                rowSum[i] += data[i][j];
            }
        }
        return rowSum;
    }

    /**
     *
     * @return col sum.
     */
    public double[] colSum() {
        int rowLen = rowTags.size();
        int colLen = colTags.size();

        double[] colSum = new double[colLen];
        for (int i = 0; i < rowLen; i++) {
            for (int j = 0; j < colLen; j++) {
                colSum[j] += data[i][j];
            }
        }

        return colSum;
    }

    /**
     *
     * @return sum.
     */
    public double sum() {
        double n = 0;
        int rowLen = rowTags.size();
        int colLen = colTags.size();
        for (int i = 0; i < rowLen; i++) {
            for (int j = 0; j < colLen; j++) {
                n += data[i][j];
            }
        }
        return n;
    }


}
