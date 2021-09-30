package com.alibaba.alink.common.sql.builtin.agg;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * agg cols to mTable
 */
public class MTableAgg extends BaseUdaf <MTable, List <Row>> {

	private String schemaStr;
	private boolean dropLast;
	private int sortColIdx;

	public MTableAgg() {
	}

	@Override
	public MTable getValue(List <Row> values) {
		MTable out = new MTable(new ArrayList <>(values), schemaStr);
		if (sortColIdx >= 0) {
			out.orderBy(sortColIdx);
		}

		if (dropLast) {
			out = new MTable(
				new ArrayList <>(out.getRows().subList(0, values.size() - 1)),
				schemaStr
			);
		}

		return out;
	}

	/**
	 *
	 * @param dropLast
	 * @return this
	 */
	public MTableAgg setDropLast(boolean dropLast) {
		this.dropLast =  dropLast;
		return this;
	}

	/**
	 *
	 * @param schemaStr: schema of output
	 * @param sortCol: if need sort, set sortCol.
	 * @return this
	 */
	public MTableAgg setSchemaStr(String schemaStr, String sortCol) {
		this.schemaStr = schemaStr;
		this.sortColIdx = sortCol == null ? -1
			: TableUtil.findColIndex(CsvUtil.schemaStr2Schema(schemaStr), sortCol);
		return this;
	}

	@Override
	public void accumulate(List <Row> acc, Object... values) {
		int len = values.length;
		Row row = new Row(len);
		for (int i = 0; i < len; i++) {
			row.setField(i, values[i]);
		}
		acc.add(row);
	}

	@Override
	public List <Row> createAccumulator() {
		return new ArrayList <Row>();
	}

	@Override
	public void retract(List <Row> lists, Object... values) {
		int len = values.length;
		Row row = new Row(len);
		for (int i = 0; i < len; i++) {
			row.setField(i, values[i]);
		}
		int index = lists.lastIndexOf(row);
		if (index >= 0) {
			lists.remove(index);
		}
	}

	@Override
	public void resetAccumulator(List <Row> lists) {
		lists.clear();
	}

	@Override
	public void merge(List <Row> lists, Iterable <List <Row>> it) {
		for (List <Row> data : it) {
			lists.addAll(data);
		}
	}
}
