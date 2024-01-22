package com.alibaba.alink.common.insights;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.sql.GroupByLocalOp2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class StatInsight {

	public static boolean isNumberType(TypeInformation<?> type) {
		return type.equals(Types.INT) || type.equals(Types.LONG) || type.equals(Types.DOUBLE) || type.equals(Types.FLOAT)  || type.equals(Types.SHORT);
	}

	public static Insight basicStat(LocalOperator <?> dataAggr, String colName) {
		Insight insight = new Insight();
		insight.type = InsightType.BasicStat;
		insight.score = 0;

		dataAggr = dataAggr.select(new String[]{colName});
		TypeInformation<?> type = dataAggr.getColTypes()[0];
		Set <Object> valueSet = new HashSet<>();
		long count = 0;
		for (Row row : dataAggr.getOutputTable().getRows()) {
			if (null != row.getField(0)) {
				valueSet.add(row.getField(0));
				count++;
			}
		}
		LayoutData layoutData = new LayoutData();
		if (!isNumberType(type)) {
			String schema = "distinct_count_value int, count_value long";
			Row row = new Row(2);
			row.setField(0, valueSet.size());
			row.setField(1, count);
			MTable mTable = new MTable(new Row[]{row}, schema);
			layoutData.data = mTable;
		} else {
			String[] outCols = new String[]{
				"distinct_count_value", "count_value", "max_value", "min_value", "sum_value", "average_value"
			};
			TypeInformation<?> [] outTypes = new TypeInformation[]{
				Types.INT, Types.LONG, type, type, Types.DOUBLE, Types.DOUBLE
			};
			TableSummary ts = dataAggr.getOutputTable().summary();

			Row row = new Row(6);
			row.setField(0, valueSet.size());
			row.setField(1, count);
			row.setField(2, ts.max(colName));
			row.setField(3, ts.min(colName));
			row.setField(4, ts.sum(colName));
			row.setField(5, ts.mean(colName));
			MTable mTable = new MTable(new Row[]{row}, outCols, outTypes);
			layoutData.data = mTable;
		}

		layoutData.title = "数据列 " + colName + " 统计数据";
		layoutData.xAxis = colName;
		insight.layout = layoutData;
		insight.score = 0.8;
		return insight;
	}

	public static Insight distribution(LocalOperator <?> dataAggr, Breakdown breakdown, String colName) {
		dataAggr = dataAggr.select(new String[] {breakdown.colName, colName});
		List <Measure> measures = new ArrayList<>();
		measures.add(new Measure(colName, MeasureAggr.COUNT));
		TypeInformation<?> type = dataAggr.getColTypes()[1];
		if (isNumberType(type)) {
			measures.add(new Measure(colName, MeasureAggr.MAX));
			measures.add(new Measure(colName, MeasureAggr.MIN));
			measures.add(new Measure(colName, MeasureAggr.SUM));
			measures.add(new Measure(colName, MeasureAggr.AVG));
		}
		String groupByClause = "`" + breakdown.colName + "`";
		StringBuilder sbdAggr = new StringBuilder();
		sbdAggr.append(groupByClause);
		for (int i = 0; i < measures.size(); i++) {
			Measure measure = measures.get(i);
			sbdAggr.append(", ").append(measure.aggr).append("(`").append(measure.colName).append("`) AS ").append(
				measure.aggr.toString().toLowerCase()).append("_value");
		}
		//System.out.println("group sql: " + sbdAggr);

		LocalOperator <?> groupByOp = new GroupByLocalOp2(groupByClause, sbdAggr.toString());
		dataAggr = dataAggr.link(groupByOp);

		Insight insight = new Insight();
		Subject subject = new Subject();
		subject.measures = measures;
		subject.breakdown = breakdown;
		insight.subject = subject;
		LayoutData layoutData = new LayoutData();
		layoutData.data = dataAggr.getOutputTable();
		layoutData.xAxis = breakdown.colName;
		layoutData.yAxis = colName + " 统计结果";
		StringBuilder titleBuilder = new StringBuilder();
		titleBuilder.append("不同 ").append(breakdown.colName).append(" 维度下，");
		titleBuilder.append(colName).append(" 的统计值");
		layoutData.title = titleBuilder.toString();

		insight.type = InsightType.Distribution;
		insight.layout = layoutData;
		insight.score = 0.8;
		return insight;
	}

	public static void distributionMulti(List <LocalOperator <?>> dataAggrs, Breakdown breakdown,
										 List<Measure> measures, List<Insight> insightList) {
		Map <String, List<Integer>> measureMap = new HashMap<>();
		int countIdx = -1;
		for (int i = 0; i < measures.size(); i++) {
			Measure m = measures.get(i);
			if (!measureMap.containsKey(m.colName)) {
				measureMap.put(m.colName, new ArrayList <>());
			}
			measureMap.get(m.colName).add(i);
			if (m.colName.equals(breakdown.colName) && m.aggr.equals(MeasureAggr.COUNT)) {
				countIdx = i;
			}
		}
		for (Entry <String, List<Integer>> entry : measureMap.entrySet()) {
			if (entry.getValue().size() <= 1) {
				continue;
			}
			List<Integer> idxList = entry.getValue();
			// 增加count的索引
			if (countIdx >= 0) {
				idxList.add(countIdx);
			}
			Insight insight = new Insight();
			Subject subject = new Subject();
			List<List<Row>> dataRows = new ArrayList <>();
			String[] colNames = new String[idxList.size() + 1];
			TypeInformation <?>[] colTypes = new TypeInformation[idxList.size() + 1];
			for (int i = 0; i < idxList.size(); i++) {
				int idx = idxList.get(i);
				subject.addMeasure(measures.get(idx));
				dataRows.add(dataAggrs.get(idx).getOutputTable().getRows());
				if (i == 0) {
					colNames[0] = dataAggrs.get(idx).getColNames()[0];
					colNames[1] = measures.get(idx).aggr.toString().toLowerCase() + "_value";
					colTypes[0] = dataAggrs.get(idx).getColTypes()[0];
					colTypes[1] = dataAggrs.get(idx).getColTypes()[1];
				} else {
					colNames[i + 1] = measures.get(idx).aggr.toString().toLowerCase() + "_value";
					colTypes[i + 1] = dataAggrs.get(idx).getColTypes()[1];
				}
			}

			subject.breakdown = breakdown;
			insight.subject = subject;
			List<Row> rows = new ArrayList <>();
			for (int i = 0; i < dataRows.get(0).size(); i++) {
				Row row = dataRows.get(0).get(i);
				Row newRow = new Row(idxList.size() + 1);
				newRow.setField(0, row.getField(0));
				newRow.setField(1, row.getField(1));
				for (int j = 1; j < dataRows.size(); j++) {
					newRow.setField(j + 1, dataRows.get(j).get(i).getField(1));
				}
				rows.add(newRow);
			}
			LayoutData layoutData = new LayoutData();
			layoutData.data = new MTable(rows, colNames, colTypes);
			layoutData.xAxis = breakdown.colName;
			layoutData.yAxis = entry.getKey() + " 统计结果";
			StringBuilder titleBuilder = new StringBuilder();
			titleBuilder.append("不同 ").append(breakdown.colName).append(" 维度下，");
			titleBuilder.append(entry.getKey()).append(" 的统计值");
			layoutData.title = titleBuilder.toString();

			insight.type = InsightType.Distribution;
			insight.layout = layoutData;
			insight.score = 0.8;
			insightList.add(insight);
		}
	}
}
