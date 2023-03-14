package com.alibaba.alink.operator.common.feature;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.operator.common.feature.binning.Bins;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsCalculator;
import com.alibaba.alink.operator.common.feature.binning.FeatureBinsUtil;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * BinningModelSummary.
 */
public class BinningModelInfo implements Serializable {
	private static final long serialVersionUID = -5091603455134141015L;
	private Map <String, List <Tuple2 <String, Long>>> categoricalScores;
	private Map <String, List <Tuple2 <String, Long>>> numericScores;

	private Map <String, List <Number>> cutsArray;

	public BinningModelInfo(List <Row> list) {
		List <FeatureBinsCalculator> featureBins = new BinningModelDataConverter().load(list);
		cutsArray = new HashMap <>();
		numericScores = new HashMap <>();
		categoricalScores = new HashMap <>();

		for (FeatureBinsCalculator featureBinsCalculator : featureBins) {
			featureBinsCalculator.splitsArrayToInterval();
			List <Tuple2 <String, Long>> map = new ArrayList <>();
			if (null != featureBinsCalculator.bin.normBins) {
				for (Bins.BaseBin bin : featureBinsCalculator.bin.normBins) {
					map.add(Tuple2.of(bin.getValueStr(featureBinsCalculator.getColType()), bin.getIndex()));
				}
			}
			if (null != featureBinsCalculator.bin.nullBin) {
				map.add(Tuple2.of(FeatureBinsUtil.NULL_LABEL, featureBinsCalculator.bin.nullBin.getIndex()));
			}
			if (null != featureBinsCalculator.bin.elseBin) {
				map.add(Tuple2.of(FeatureBinsUtil.ELSE_LABEL, featureBinsCalculator.bin.elseBin.getIndex()));
			}
			if (featureBinsCalculator.isNumeric()) {
				numericScores.put(featureBinsCalculator.getFeatureName(), map);
				cutsArray.put(featureBinsCalculator.getFeatureName(),
					Arrays.asList(featureBinsCalculator.getSplitsArray()));
			} else {
				categoricalScores.put(featureBinsCalculator.getFeatureName(), map);
			}
		}
	}

	public String[] getCategoryColumns() {
		return categoricalScores.keySet().toArray(new String[0]);
	}

	public String[] getContinuousColumns() {
		return cutsArray.keySet().toArray(new String[0]);
	}

	public int getCategorySize(String columnName) {
		Integer binCount = categoricalScores.get(columnName).size() - 2;
		Preconditions.checkNotNull(binCount, columnName + "is not discrete column!");
		return binCount;
	}

	public Number[] getCutsArray(String columnName) {
		Number[] cuts = cutsArray.get(columnName).toArray(new Number[0]);
		Preconditions.checkNotNull(cuts, columnName + "is not continuous column!");
		return cuts;
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder(PrettyDisplayUtils.displayHeadline("BinningModelSummary", '-'));
		sbd.append("Binning on ")
			.append(categoricalScores.size() + numericScores.size())
			.append(" features.\n")
			.append("Categorical features:")
			.append(PrettyDisplayUtils.displayList(new ArrayList <>(categoricalScores.keySet()), 3, false))
			.append("\nNumeric features:")
			.append(PrettyDisplayUtils.displayList(new ArrayList <>(numericScores.keySet()), 3, false))
			.append("\n")
			.append(PrettyDisplayUtils.displayHeadline("Details", '-'));
		int size = categoricalScores.values().stream().mapToInt(m -> m.size()).sum() +
			numericScores.values().stream().mapToInt(m -> m.size()).sum();
		String[][] table = new String[size][3];
		int cnt = 0;
		for (Map.Entry <String, List <Tuple2 <String, Long>>> entry : categoricalScores.entrySet()) {
			table[cnt][0] = entry.getKey();
			for (Tuple2 <String, Long> entry1 : entry.getValue()) {
				if (table[cnt][0] == null) {
					table[cnt][0] = "";
				}
				table[cnt][1] = entry1.f0;
				table[cnt++][2] = entry1.f1.toString();
			}
		}
		for (Map.Entry <String, List <Tuple2 <String, Long>>> entry : numericScores.entrySet()) {
			table[cnt][0] = entry.getKey();
			for (Tuple2 <String, Long> entry1 : entry.getValue()) {
				if (table[cnt][0] == null) {
					table[cnt][0] = "";
				}
				table[cnt][1] = entry1.f0;
				table[cnt++][2] = entry1.f1.toString();
			}
		}
		sbd.append(PrettyDisplayUtils.displayTable(table, table.length, 3, null,
			new String[] {"featureName", "FeatureBin", "BinIndex"}, null, 20, 3));
		return sbd.toString();
	}
}
