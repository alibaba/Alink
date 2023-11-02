package com.alibaba.alink.common.insights;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.alink.common.insights.Mining.MEASURE_NAME_PREFIX;

public class AggregationQuery {

	public static List <LocalOperator <?>> query(LocalOperator <?> subData,
												 Breakdown breakdown,
												 List <Measure> measures) {
		String breakdownCol = breakdown.colName;
		List <LocalOperator <?>> result = new ArrayList <>();

		LocalOperator <?> dataQuery;
		if (AutoDiscovery.isTimestampCol(subData.getSchema(), breakdownCol)) {
			String tmpTsCol = "__alink_ts_tmp__";
			StringBuilder sbdAggr = new StringBuilder();
			sbdAggr.append(tmpTsCol);
			String[] measureCols = new String[measures.size()];
			for (int i = 0; i < measures.size(); i++) {
				Measure measure = measures.get(i);
				measureCols[i] = MEASURE_NAME_PREFIX + i;
				sbdAggr.append(", ").append(measure.aggr).append("(`").append(measure.colName).append("`) AS ").append(
					measureCols[i]);
			}
			String selectSql1 = String.format("unix_timestamp_macro(%s) as %s, *", breakdownCol, tmpTsCol);
			String groupSql = sbdAggr.toString();
			String selectSql2 = String.format("to_timestamp_micro(%s) as %s, %s", tmpTsCol, breakdownCol,
				String.join(",", measureCols));

			System.out.println(selectSql1);
			System.out.println(groupSql);
			System.out.println(selectSql2);

			dataQuery = subData
				.select(selectSql1)
				.groupBy(tmpTsCol, groupSql)
				.select(selectSql2);
		} else {
			String groupByClause = "`" + breakdownCol + "`";
			StringBuilder sbdAggr = new StringBuilder();
			sbdAggr.append(groupByClause);
			for (int i = 0; i < measures.size(); i++) {
				Measure measure = measures.get(i);
				sbdAggr.append(", ").append(measure.aggr).append("(`").append(measure.colName).append("`) AS ").append(
					MEASURE_NAME_PREFIX).append(i);
			}
			System.out.println("group sql: " + sbdAggr);
			dataQuery = subData.groupBy(groupByClause, sbdAggr.toString());
		}
		if (measures.size() == 1) {
			result.add(dataQuery);
		} else {
			String[] parts = TableUtil.schema2SchemaStr(dataQuery.getSchema()).split(",");
			MTable mt = dataQuery.getOutputTable();
			for (int i = 1; i <= measures.size(); i++) {
				int[] colIndexes = new int[] {0, i};
				ArrayList <Row> rows = new ArrayList <>();
				for (Row r : mt.getRows()) {
					rows.add(Row.project(r, colIndexes));
				}

				result.add(new MemSourceLocalOp(
					new MTable(rows, parts[0] + ", " + MEASURE_NAME_PREFIX + "0 " + parts[i].trim().split(" ")[1])));
			}
		}
		return result;
	}

	public static LocalOperator <?> query(LocalOperator <?> source,
										  List <Subspace> subspaces,
										  String expandingColName,
										  List <Object> expandingValues,
										  Breakdown breakdown,
										  List <Measure> measures) {
		//source = Mining.filter(source, subspaces);

		if (subspaces.size() > 0) {
			StringBuilder sbd = new StringBuilder();
			sbd.append(createFilterSubspaceClause(subspaces.get(0)));
			for (int i = 1; i < subspaces.size(); i++) {
				sbd.append(" AND ").append(createFilterSubspaceClause(subspaces.get(i)));
			}
			if (null != expandingColName && null != expandingValues && expandingValues.size() > 0) {
				sbd.append(" AND ").append(createFilterExpandingClause(expandingColName, expandingValues));
			}
			System.out.println("filter sql: " + sbd.toString());
			source = source.filter(sbd.toString());
		}

		String groupByClause = "`" + breakdown.colName + "`";
		if (null != expandingColName) {
			groupByClause += ", `" + expandingColName + "`";
		}
		StringBuilder sbdAggr = new StringBuilder();
		sbdAggr.append(groupByClause);
		for (int i = 0; i < measures.size(); i++) {
			Measure measure = measures.get(i);
			sbdAggr.append(", ").append(measure.aggr).append("(`").append(measure.colName).append("`) AS ").append(
				MEASURE_NAME_PREFIX).append(i);
		}

		//System.out.println("group sql: " + sbdAggr);
		LocalOperator <?> dataAggr = source.groupBy(groupByClause, sbdAggr.toString());
		return dataAggr;
	}

	private static String createFilterSubspaceClause(Subspace subspace) {
		if (subspace.value instanceof String) {
			return subspace.colName + "='" + subspace.value + "'";
		} else {
			return subspace.colName + "=" + subspace.value;
		}
	}

	private static String createFilterExpandingClause(String expandingColName, List <Object> expandingValues) {
		StringBuilder sbd = new StringBuilder();
		sbd.append("`").append(expandingColName).append("` IN (");
		if (expandingValues.get(0) instanceof String) {
			sbd.append("'").append(expandingValues.get(0)).append("'");
			for (int i = 1; i < expandingValues.size(); i++) {
				sbd.append(", '").append(expandingValues.get(i)).append("'");
			}
		} else {
			sbd.append(expandingValues.get(0));
			for (int i = 1; i < expandingValues.size(); i++) {
				sbd.append(", ").append(expandingValues.get(i));
			}
		}
		sbd.append(")");
		return sbd.toString();
	}

}
