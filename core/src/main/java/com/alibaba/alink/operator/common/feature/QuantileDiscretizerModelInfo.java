package com.alibaba.alink.operator.common.feature;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsUtil;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Summary of Quantile Discretizer Model;
 */
public class QuantileDiscretizerModelInfo implements Serializable {
    private Map<String, List<Number>> cutsArray;
    private Map<String, List<String>> interval;

    public QuantileDiscretizerModelInfo(List<Row> list) {
        cutsArray = new HashMap<>();
        interval = new HashMap<>();
        for (Map.Entry<String, ContinuousRanges> entry : (new QuantileDiscretizerModelDataConverter().load(
            list).data).entrySet()) {
            cutsArray.put(entry.getKey(), Arrays.asList(entry.getValue().splitsArray));
            interval.put(entry.getKey(), Arrays.asList(
                FeatureBinsUtil.cutsArrayToInterval(entry.getValue().splitsArray, entry.getValue().getLeftOpen())));
        }
    }

    public String[] getSelectedColsInModel() {
        return cutsArray.keySet().toArray(new String[0]);
    }

    public Number[] getCutsArray(String columnName) {
        Preconditions.checkState(cutsArray.containsKey(columnName), columnName + "is not contained in the model!");
        return cutsArray.get(columnName).toArray(new Number[0]);
    }

    @Override
    public String toString() {
        StringBuilder sbd = new StringBuilder(PrettyDisplayUtils.displayHeadline("QuantileDiscretizerModelInfo", '-'));
        sbd.append("Quantile discretizes on ")
            .append(cutsArray.size())
            .append(" features: ")
            .append(PrettyDisplayUtils.displayList(new ArrayList<>(cutsArray.keySet()), 3, false))
            .append("\n")
            .append(mapToString(interval));
        return sbd.toString();
    }

    static String mapToString(Map<String, List<String>> interval) {
        StringBuilder sbd = new StringBuilder();
        sbd.append(PrettyDisplayUtils.displayHeadline("Details", '='));
        sbd.append(PrettyDisplayUtils.displayMap(interval, 3, true));
        sbd.append("\n");
        return sbd.toString();
    }
}
