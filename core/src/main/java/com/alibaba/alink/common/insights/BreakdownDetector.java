package com.alibaba.alink.common.insights;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.local.LocalOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class BreakdownDetector {

	List <Tuple2 <Breakdown, List <Measure>>> list = new ArrayList <>();

	public BreakdownDetector() {
	}

	public BreakdownDetector detect(LocalOperator <?> table, List <Subspace> subspaces) {
		LocalOperator <?> data = Mining.filter(table, subspaces);

		Set <String> breakdownColNames = new TreeSet <>();
		for (String name : data.getColNames()) {
			if (!TableUtil.isSupportedNumericType(table.getSchema().getFieldType(name).get())) {
				breakdownColNames.add(name);
			}
		}
		// breakdown column CAN NOT be subspace column.
		for (Subspace subspace : subspaces) {
			breakdownColNames.remove(subspace.colName);
		}

		Set <String> allMeasureColNames = new TreeSet <>();
		for (String colName : TableUtil.getNumericCols(data.getSchema())) {
			allMeasureColNames.add(colName);
		}
		// measure column CAN NOT be subspace column.
		for (Subspace subspace : subspaces) {
			allMeasureColNames.remove(subspace.colName);
		}

		final int totalCount = data.getOutputTable().getNumRow();
		for (String breakdownColName : breakdownColNames) {
			int distinctCount = 0;

			if (AutoDiscovery.isTimestampCol(table.getSchema(), breakdownColName)) {
				distinctCount = data
					.select(String.format("unix_timestamp_macro(%s) as %s", breakdownColName, breakdownColName))
					.distinct()
					.getOutputTable()
					.getNumRow();
			} else {
				distinctCount = data
					.select(new String[] {breakdownColName})
					.distinct()
					.getOutputTable()
					.getNumRow();
			}

			List <Measure> measures = new ArrayList <>();
			Set <String> measureColNames = new TreeSet <>(allMeasureColNames);
			measureColNames.remove(breakdownColName);
			if (totalCount == distinctCount) {
				for (String measureColName : measureColNames) {
					measures.add(new Measure(measureColName, MeasureAggr.SUM));
				}
				list.add(Tuple2.of(new Breakdown(breakdownColName), measures));
			} else {
				measures.add(new Measure(breakdownColName, MeasureAggr.COUNT));
				for (String measureColName : measureColNames) {
					measures.add(new Measure(measureColName, MeasureAggr.SUM));
					measures.add(new Measure(measureColName, MeasureAggr.AVG));
					measures.add(new Measure(measureColName, MeasureAggr.MIN));
					measures.add(new Measure(measureColName, MeasureAggr.MAX));
				}
				list.add(Tuple2.of(new Breakdown(breakdownColName), measures));
			}
		}

		return this;
	}
}