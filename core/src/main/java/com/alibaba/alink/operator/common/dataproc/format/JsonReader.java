package com.alibaba.alink.operator.common.dataproc.format;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.JsonConverter;

import java.util.Map;

public class JsonReader extends FormatReader {

	private static final long serialVersionUID = -5403483533106321191L;
	final int jsonColIndex;

	public JsonReader(int jsonColIndex) {
		this.jsonColIndex = jsonColIndex;
	}

	@Override
	boolean read(Row row, Map <String, String> out) {
		String line = (String) row.getField(jsonColIndex);
		try {
			Map map = JsonConverter.fromJson(line, Map.class);

			map.forEach((key, value) -> {
				if (null != value) {
					if (value instanceof Map) {
						out.put(key.toString(), JsonConverter.toJson(value));
					} else {
						out.put(key.toString(), value.toString());
					}
				} else {
					out.put(key.toString(), null);
				}
			});
			return true;
		} catch (Exception e) {
			return false;
		}

	}
}
