package com.alibaba.alink.common;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.TableUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MTableUtil implements Serializable {

	public static List <Object> getColumn(MTable mTable, String colName) {
		String[] names = mTable.getColNames();
		int idx = TableUtil.findColIndex(names, colName);
		if (idx == -1) {
			return null;
		}
		List <Row> table = mTable.getRows();
		List <Object> ret = new ArrayList <>(table.size());
		for (int i = 0; i < table.size(); ++i) {
			ret.add(i, table.get(i).getField(idx));
		}
		return ret;
	}

	public static Map <String, List <Object>> getColumns(MTable mTable) {
		List <Row> table = mTable.getRows();
		String[] names = mTable.getColNames();
		Map <String, List <Object>> ret = new HashMap <>(names.length);
		for (String name : names) {
			int idx = TableUtil.findColIndex(names, name);
			List <Object> objects = new ArrayList <>(table.size());
			for (int i = 0; i < table.size(); ++i) {
				objects.add(i, table.get(i).getField(idx));
			}
			ret.put(name, objects);
		}
		return ret;
	}

	public static MTable getMTable(Object obj) {
		if (obj == null) {
			return null;
		}
		if (obj instanceof MTable) {
			return (MTable) obj;
		} else if (obj instanceof String) {
			return MTable.fromJson((String) obj);
		} else {
			throw new RuntimeException("Type must be string or mtable");
		}

	}

	static MTable copy(MTable mt) {
		ArrayList <Row> rows = new ArrayList <>(mt.getRows().size());
		for (Row r : mt.getRows()) {
			rows.add(Row.copy(r));
		}
		return new MTable(rows, mt.getSchemaStr());
	}

	static MTable select(MTable mt, String[] colNames) {
		return select(mt, colNames, TableUtil.findColIndicesWithAssertAndHint(mt.getSchema(), colNames));
	}

	static MTable select(MTable mt, int[] colIndexes) {
		String[] colAllNames = mt.getColNames();
		String[] colNames = new String[colIndexes.length];
		for (int i = 0; i < colIndexes.length; i++) {
			colNames[i] = colAllNames[colIndexes[i]];
		}
		return select(mt, colNames, colIndexes);
	}

	private static MTable select(MTable mt, String[] colNames, int[] colIndexes) {
		ArrayList <Row> rows = new ArrayList <>();
		for (Row r : mt.getRows()) {
			rows.add(Row.project(r, colIndexes));
		}
		return new MTable(rows, colNames, TableUtil.findColTypesWithAssertAndHint(mt.getSchema(), colNames));
	}
}
