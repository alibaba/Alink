package com.alibaba.alink.operator.common.dataproc;

import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.alink.operator.common.utils.PrettyDisplayUtils.displayList;

public class MinMaxScalerModelInfo {
    double[] eMins;
    double[] eMaxs;
    MinMaxScalerModelInfo(List<Row> rows) {
        Tuple4<Double, Double, double[], double[]> tuple4 = new MinMaxScalerModelDataConverter().load(rows);
        eMins = tuple4.f2;
        eMaxs = tuple4.f3;
    }

    public double[] getEMins() {
        return eMins;
    }

    public double[] getEMaxs() {
        return eMaxs;
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();
        res.append(PrettyDisplayUtils.displayHeadline("MinMaxScalerModelInfo", '-')+"\n");
        res.append(PrettyDisplayUtils.displayHeadline("lower bound information", '=')+"\n");
        res.append(displayList(Arrays.asList(ArrayUtils.toObject(eMins)), false) + "\n");
        res.append(PrettyDisplayUtils.displayHeadline("upper bound information", '=')+"\n");
        res.append(displayList(Arrays.asList(ArrayUtils.toObject(eMaxs)), false) + "\n");
        return res.toString();
    }
}
