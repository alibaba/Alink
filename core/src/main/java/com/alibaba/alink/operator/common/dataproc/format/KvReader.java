package com.alibaba.alink.operator.common.dataproc.format;

import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import java.util.Map;

public class KvReader extends FormatReader {

	private static final long serialVersionUID = -6643032534147603545L;
	final int kvColIndex;
	final String colDelimiter;
	final String valDelimiter;

	public KvReader(int kvColIndex, String colDelimiter, String valDelimiter) {
		this.kvColIndex = kvColIndex;
		this.colDelimiter = colDelimiter;
		this.valDelimiter = valDelimiter;
	}

	@Override
	boolean read(Row row, Map <String, String> out) {
		String line = (String) row.getField(kvColIndex);
		String[] fields = line.split(colDelimiter);

		for (int i = 0; i < fields.length; i++) {
			if (StringUtils.isNullOrWhitespaceOnly(fields[i])) {
				return false;
			}
			String[] kv = fields[i].split(valDelimiter);
			if (kv.length != 2) {
				return false;
			}
			out.put(kv[0], kv[1]);
		}

		return true;
	}
}
