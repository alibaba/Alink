package com.alibaba.alink.operator.common.dataproc.format;

import org.apache.flink.types.Row;

import java.util.Map;

public class ColumnsReader extends FormatReader {

	final String[] colNames;
	final int[] colIndices;

	public ColumnsReader(int[] colIndices, String[] colNames) {
		this.colNames = colNames;
		this.colIndices = colIndices;
	}

	@Override
	boolean read(Row row, Map <String, String> out) {
		for (int i = 0; i < colNames.length; i++) {
			Object obj = row.getField(colIndices[i]);
			if (null != obj) {
				out.put(colNames[i], obj.toString());
			}
		}
		return true;
	}

}
