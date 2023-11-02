package com.alibaba.alink.common.insights;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.local.LocalOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class ImpactDetector {
	final double threshold;

	String[] colNames;
	Map <String, Integer> colNameMap;
	Map <Object, Double>[] valueMaps;

	public ImpactDetector(double threshold) {
		this.threshold = threshold;
	}

	public void detect(LocalOperator <?> table) {
		colNames = table.getColNames();
		colNameMap = new HashMap <>(colNames.length);
		valueMaps = new Map[colNames.length];
		for (int i = 0; i < colNames.length; i++) {
			colNameMap.put(colNames[i], i);
			valueMaps[i] = new HashMap <>();
		}
		final int totalCount = table.getOutputTable().getNumRow();
		final int minCount = (int) Math.round(totalCount * this.threshold);
		for (int i = 0; i < colNames.length; i++) {
			String colName = "`" + colNames[i] + "`";
			List <Row> rows;
			if (AutoDiscovery.isTimestampCol(table.getSchema(), colNames[i])) {
				String tmpTsCol = "__alink_ts_tmp__";
				String selectSql1 = String.format("unix_timestamp_macro(%s) as %s, *", colName, tmpTsCol);
				String groupSql = tmpTsCol + ", COUNT(" + tmpTsCol + ") AS cnt";
				String selectSql2 = String.format("to_timestamp_micro(%s) as %s, %s", tmpTsCol, colName, "cnt");
				rows = table
					.select(selectSql1)
					.groupBy(tmpTsCol, groupSql)
					.select(selectSql2)
					.filter("cnt>=" + minCount)
					.getOutputTable()
					.getRows();
			} else {
				rows = table
					.groupBy(colName, colName + ", COUNT(" + colName + ") AS cnt")
					.filter("cnt>=" + minCount)
					.getOutputTable()
					.getRows();
			}
			for (Row row : rows) {
				valueMaps[i].put(row.getField(0), ((Number) row.getField(1)).doubleValue() / totalCount);
			}
		}
	}

	public double predict(Subspace subspace) {
		Integer k = colNameMap.get(subspace.colName);
		if (null != k) {
			Double value = valueMaps[k].get(subspace.value);
			if (null != value) {
				return value;
			}
		}
		return 0.0;
	}

	public List <Tuple2 <Subspace, Double>> listSingleSubspace() {
		List <Tuple2 <Subspace, Double>> result = new ArrayList <>();

		for (Entry <String, Integer> name_index : colNameMap.entrySet()) {
			String colName = name_index.getKey();
			Map <Object, Double> valueMap = valueMaps[name_index.getValue()];
			for (Entry <Object, Double> value_impact : valueMap.entrySet()) {
				result.add(Tuple2.of(new Subspace(colName, value_impact.getKey()), value_impact.getValue()));
			}
		}

		Collections.sort(result, new Comparator <Tuple2 <Subspace, Double>>() {
			@Override
			public int compare(Tuple2 <Subspace, Double> o1, Tuple2 <Subspace, Double> o2) {
				return -o1.f1.compareTo(o2.f1);
			}
		});
		return result;
	}

	public List <Tuple2 <Subspace, Double>> listSingleShapeSubspace() {
		List <Tuple2 <Subspace, Double>> result = new ArrayList <>();

		for (Entry <String, Integer> name_index : colNameMap.entrySet()) {
			String colName = name_index.getKey();
			Map <Object, Double> valueMap = valueMaps[name_index.getValue()];
			for (Entry <Object, Double> value_impact : valueMap.entrySet()) {
				result.add(Tuple2.of(new Subspace(colName, value_impact.getKey()), value_impact.getValue()));
			}
		}

		Collections.sort(result, new Comparator <Tuple2 <Subspace, Double>>() {
			@Override
			public int compare(Tuple2 <Subspace, Double> o1, Tuple2 <Subspace, Double> o2) {
				return -o1.f1.compareTo(o2.f1);
			}
		});
		return result;
	}

	public List <Tuple3 <Subspace, Subspace, Double>> searchDoubleSubspace(LocalOperator <?> table) {
		List <Tuple3 <Subspace, Subspace, Double>> result = new ArrayList <>();

		List <Integer> possibleIndexes = new ArrayList <>();
		for (int i = 0; i < valueMaps.length; i++) {
			if (valueMaps[i].size() > 0) {
				possibleIndexes.add(i);
			}
		}
		final int totalCount = table.getOutputTable().getNumRow();
		final int minCount = (int) Math.round(totalCount * this.threshold);
		for (int i = 0; i < possibleIndexes.size(); i++) {
			String name1 = "`" + colNames[possibleIndexes.get(i)] + "`";
			for (int j = i + 1; j < possibleIndexes.size(); j++) {
				String name2 = "`" + colNames[possibleIndexes.get(j)] + "`";
				List <Row> rows = table
					.groupBy(name1 + "," + name2, name1 + "," + name2 + ", COUNT(" + name1 + ") AS cnt")
					.filter("cnt>=" + minCount)
					.getOutputTable()
					.getRows();

				for (Row row : rows) {
					result.add(Tuple3.of(
						new Subspace(name1, row.getField(0)),
						new Subspace(name2, row.getField(1)),
						((Number) row.getField(2)).doubleValue() / totalCount
					));
				}
			}
		}

		Collections.sort(result, new Comparator <Tuple3 <Subspace, Subspace, Double>>() {
			@Override
			public int compare(Tuple3 <Subspace, Subspace, Double> o1, Tuple3 <Subspace, Subspace, Double> o2) {
				return -o1.f2.compareTo(o2.f2);
			}
		});
		return result;
	}

	private static int[] getTimestampCols(TableSchema schema) {
		List <Integer> tsCols = new ArrayList <>();
		for (int i = 0; i < schema.getFieldNames().length; i++) {
			if (Types.SQL_TIMESTAMP == schema.getFieldType(i).get()) {
				tsCols.add(i);
			}
		}
		int[] tsColIndices = new int[tsCols.size()];
		for (int i = 0; i < tsColIndices.length; i++) {
			tsColIndices[i] = tsCols.get(i);
		}
		return tsColIndices;
	}

}
