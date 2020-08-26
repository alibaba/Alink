package com.alibaba.alink.operator.common.linearprogramming;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.VectorIterator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LinearProgrammingUtil {

    /**
     * print a vector
     */
    public static void LPPrintVector(DenseVector vector) {
        for (int i = 0; i < vector.size(); i++)
            System.out.printf("%.2f ", vector.get(i));
        System.out.print("\n");
    }

    /**
     * print the simplex tableau
     */
    public static void LPPrintTableau(List<Tuple2<Integer, DenseVector>> tableau) {
        for (Tuple2<Integer, DenseVector> t : tableau) {
            System.out.printf("x_%d\t= %.2f\t", t.f0, t.f1.get(0));
        }

        System.out.println();

        for (Tuple2<Integer, DenseVector> t : tableau)
            LPPrintVector(t.f1);
    }

    public static SparseVector listRow2Vector(List<Row> listRow) {
        if (listRow == null)
            return null;
        int listLength = listRow.size();
        SparseVector sparseVector = new SparseVector(listLength);
        for (int i = 0; i < listLength; i++)
            sparseVector.set(
                    (Integer) listRow.get(i).getField(0),
                    (Double) listRow.get(i).getField(1));
        return sparseVector;
    }

    public static Integer[] listRow2listInt(List<Row> listRow) {
        if (listRow == null)
            return null;
        int listLength = listRow.size();
        Integer[] listInt = new Integer[listLength];
        for (int i = 0; i < listLength; i++)
            listInt[i] = (Integer) listRow.get(i).getField(0);
        return listInt;
    }
}
