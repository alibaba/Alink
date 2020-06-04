package com.alibaba.alink.operator.common.dataproc.format;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.Map;

public class CsvWriter extends FormatWriter {

	private final String fieldDelim;
	private final String quoteString;
	private final String escapedQuote;
	private final boolean enableQuote;
	final String[] colNames;

	public CsvWriter(TableSchema schema, String fieldDelim, Character quoteChar) {

		this.colNames = schema.getFieldNames();
		this.fieldDelim = fieldDelim;
		this.enableQuote = quoteChar != null;
		if (enableQuote) {
			this.quoteString = quoteChar.toString();
			this.escapedQuote = this.quoteString + this.quoteString;
		} else {
			this.quoteString = null;
			this.escapedQuote = null;
		}
	}

	@Override
	public Tuple2 <Boolean, Row> write(Map <String, String> in) {
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < colNames.length; i++) {
			if (i > 0) {
				sbd.append(fieldDelim);
			}
			String v = in.get(colNames[i]);
			if (v == null) {
				continue;
			}
			if (quoteString != null) {
				if (v.isEmpty() || v.contains(fieldDelim) || v.contains(quoteString)) {
					sbd.append(quoteString);
					sbd.append(v.replace(quoteString, this.escapedQuote + quoteString));
					sbd.append(quoteString);
				} else {
					sbd.append(v);
				}
			} else {
				sbd.append(v);
			}
		}

		return new Tuple2 <>(true, Row.of(sbd.toString()));
	}
}
