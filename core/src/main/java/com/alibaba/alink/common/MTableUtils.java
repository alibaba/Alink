package com.alibaba.alink.common;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.TableUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MTableUtils implements Serializable {

	public static List <Object> getColumn(MTable mTable, String colName) {
		List <Row> table = mTable.getTable();
		String[] names = mTable.getColNames();
		int idx = TableUtil.findColIndex(names, colName);
		List <Object> ret = new ArrayList <>(table.size());
		for (int i = 0; i < table.size(); ++i) {
			ret.add(i, table.get(i).getField(idx));
		}
		return ret;
	}

	public static Map <String, List <Object>> getColumns(MTable mTable) {
		List <Row> table = mTable.getTable();
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
		} else if(obj instanceof String) {
			return new MTable((String) obj);
		} else {
			throw new RuntimeException("Type must be string or mtable");
		}

	}
}
