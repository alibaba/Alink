package com.alibaba.alink.common.pyrunner;

import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;

public class DataConversionUtils {

	public static Object[] rowToObjectArray(Row row) {
		Object[] arr = new Object[row.getArity()];
		for (int i = 0; i < row.getArity(); i += 1) {
			arr[i] = row.getField(i);
		}
		return arr;
	}

	public static Object[][] listRowToObject2DArray(Row[] rows) {
		return Arrays.stream(rows)
			.map(DataConversionUtils::rowToObjectArray)
			.toArray(Object[][]::new);
	}

	public static Row objectArrayToRow(Object[] data) {
		return Row.of(data);
	}

	public static Row objectArrayToRow(List<Object> data) {
		return Row.of(data.toArray(new Object[0]));
	}

	public static Row[] object2DArrayToListRow(Object[][] data) {
		return Arrays.stream(data)
			.map(DataConversionUtils::objectArrayToRow)
			.toArray(Row[]::new);
	}
	public static Row[] object2DArrayToListRow(List<List <Object>> data) {
		return data.stream()
			.map(DataConversionUtils::objectArrayToRow)
			.toArray(Row[]::new);
	}
}
