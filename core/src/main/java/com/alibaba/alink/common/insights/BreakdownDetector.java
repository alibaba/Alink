package com.alibaba.alink.common.insights;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.local.AlinkLocalSession;
import com.alibaba.alink.operator.local.AlinkLocalSession.TaskRunner;
import com.alibaba.alink.operator.local.LocalOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BreakdownDetector {

	List <Tuple2 <Breakdown, List <Measure>>> list = new ArrayList <>();

	public BreakdownDetector() {
	}

	public List <Tuple2 <Breakdown, List <Measure>>> getBreakdownAndMeasures() {
		return list;
	}

	// return <breakdown cols, measure cols>

	public BreakdownDetector detect(LocalOperator <?> table,
									List <Subspace> subspaces,
									boolean sameSubspaceCol,
									int distinctCountThreshold,
									int threadNum) {
		return detect(table, BreakdownDetector.getBreakdownCols(table.getSchema()),
			subspaces, sameSubspaceCol, distinctCountThreshold, threadNum);
	}

	public BreakdownDetector detect(LocalOperator <?> table,
									String[] breakdownCols,
									List <Subspace> subspaces,
									boolean sameSubspaceCol,
									int distinctCountThreshold,
									int threadNum) {
		//System.out.println("breakdown thread num: " + threadNum);
		LocalOperator <?> data;
		if (!sameSubspaceCol) {
			data = Mining.filter(table, subspaces);
		} else {
			data = table;
		}

		Tuple2 <Set <String>, Set <String>> t2 = getBreakdownAndMeasureCols(subspaces, table.getSchema(),
			breakdownCols);

		// breakdownColNames less than breakdownCols
		Set <String> breakdownColNames = t2.f0;
		Set <String> allMeasureColNames = t2.f1;

		final int totalCount = data.getOutputTable().getNumRow();
		final String[] breakdownColNameList = breakdownColNames.toArray(new String[0]);
		int breakDownColNum = breakdownColNameList.length;
		int[] breakDownColDistinctCount = new int[breakDownColNum];

		//System.out.println("detect thread num: " + threadNum);

		if (threadNum == 1) {
			Set <Object> sets = new HashSet <>();
			for (int j = 0; j < breakDownColNum; j++) {
				sets.clear();
				breakDownColDistinctCount[j] = distinctCount(data, breakdownColNameList[j],
					distinctCountThreshold, sets);
			}
		} else {
			final TaskRunner taskRunner = new TaskRunner();
			for (int i = 0; i < threadNum; ++i) {
				final int start = (int) AlinkLocalSession.DISTRIBUTOR.startPos(i, threadNum, breakDownColNum);
				final int cnt = (int) AlinkLocalSession.DISTRIBUTOR.localRowCnt(i, threadNum, breakDownColNum);

				if (cnt <= 0) {continue;}

				taskRunner.submit(() -> {
					Set <Object> sets = new HashSet <>();
					for (int j = start; j < start + cnt; j++) {
						sets.clear();
						breakDownColDistinctCount[j] = distinctCount(data, breakdownColNameList[j],
							distinctCountThreshold, sets);
					}
				});
			}

			taskRunner.join();
		}

		list.addAll(calcBreakdownAndMeasures(breakdownColNameList,
			allMeasureColNames,
			breakDownColDistinctCount,
			totalCount,
			distinctCountThreshold
		));

		return this;
	}

	public static Tuple2 <Set <String>, Set <String>> getBreakdownAndMeasureCols(List <Subspace> subspaces,
																				 TableSchema tableSchema,
																				 String[] breakdownCols) {
		Set <String> breakdownColNames = new HashSet <>(Arrays.asList(breakdownCols));

		// breakdown column CAN NOT be subspace column.
		for (Subspace subspace : subspaces) {
			breakdownColNames.remove(subspace.colName);
		}

		Set <String> allMeasureColNames = new HashSet <>(Arrays.asList(TableUtil.getNumericCols(tableSchema)));

		// measure column CAN NOT be subspace column.
		for (Subspace subspace : subspaces) {
			allMeasureColNames.remove(subspace.colName);
		}
		return Tuple2.of(breakdownColNames, allMeasureColNames);
	}

	// category and timestamp type.
	public static String[] getBreakdownCols(TableSchema tableSchema) {
		String[] colNames = tableSchema.getFieldNames();
		TypeInformation <?>[] colTypes = tableSchema.getFieldTypes();
		List <String> res = new ArrayList <>();
		for (int i = 0; i < colNames.length; i++) {
			if (Types.SQL_TIMESTAMP == colTypes[i] || Types.STRING == colTypes[i]) {
				res.add(colNames[i]);
			}
		}
		return res.toArray(new String[0]);
	}

	public static List <Tuple2 <Breakdown, List <Measure>>> calcBreakdownAndMeasures(
		String[] breakdownColNameList,
		Set <String> allMeasureColNames,
		int[] breakDownColDistinctCount,
		int totalCount,
		int distinctCountThreshold
	) {
		List <Tuple2 <Breakdown, List <Measure>>> list = new ArrayList <>();

		int breakDownColNum = breakdownColNameList.length;
		for (int i = 0; i < breakDownColNum; i++) {
			String breakdownColName = breakdownColNameList[i];
			int distinctCount = breakDownColDistinctCount[i];
			if (distinctCount > distinctCountThreshold ||
				distinctCount < 2) {
				continue;
			}

			List <Measure> measures = new ArrayList <>();
			Set <String> measureColNames = new HashSet <>(allMeasureColNames);
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
		return list;
	}

	public static int distinctCount(LocalOperator <?> in,
									String colName,
									int distinctCountThreshold,
									Set <Object> sets) {
		int colIdx = TableUtil.findColIndex(in.getSchema(), colName);
		return distinctCount(in.getOutputTable().getRows(), colIdx, distinctCountThreshold, sets);
	}

	public static int distinctCount(Iterable <Row> dataRows,
									int colIdx,
									int distinctCountThreshold,
									Set <Object> sets) {
		for (Row row : dataRows) {
			Object obj = row.getField(colIdx);
			if (obj != null) {
				sets.add(obj);
				if (sets.size() > distinctCountThreshold) {
					return distinctCountThreshold + 1;
				}
			}
		}

		return sets.size();
	}

}