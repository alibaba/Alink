package com.alibaba.alink.operator.common.statistics.statistics;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.TableUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WindowTable {

	public String[] colNames;
	public Class[] colTypes;
	private Iterable <Row> list = null;
	private Iterator <Row> iter = null;
	private String groupColName;
	private String groupValue;

	public WindowTable(String[] colNames, Class[] colTypes, Iterable <Row> list) {
		this.colNames = colNames.clone();
		this.colTypes = colTypes.clone();
		this.list = list;
	}

	public WindowTable(String[] colNames, Class[] colTypes, Iterable <Row> list, String groupColName,
					   String groupValue) {
		this.colNames = colNames.clone();
		this.colTypes = colTypes.clone();
		this.list = list;
		this.groupColName = groupColName;
		this.groupValue = groupValue;
	}

	public Iterator <Row> getIterator() {
		if (iter != null) {
			return iter;
		}
		if (groupColName == null || groupColName.isEmpty()) {
			return list.iterator();
		} else {
			int groupIdx = TableUtil.findColIndex(this.colNames, groupColName);
			List <Row> valuesByKey = new ArrayList <>();
			Iterator <Row> iter = list.iterator();
			while (iter.hasNext()) {
				Row row = iter.next();
				Object field = row.getField(groupIdx);
				if (field == null && groupValue == null) {
					valuesByKey.add(row);
				} else if (field == null || groupValue == null) {
					//pass
				} else if (field.toString().equals(groupValue)) {
					valuesByKey.add(row);
				}
			}
			return valuesByKey.iterator();
		}
	}

	public int getColumnCount() {
		return this.colTypes.length;
	}

	public String[] getColNames() {
		return colNames;
	}

	public Class[] getColTypes() {
		return colTypes;
	}

	public String getColumnName(int column) {
		return colNames[column];
	}

}
