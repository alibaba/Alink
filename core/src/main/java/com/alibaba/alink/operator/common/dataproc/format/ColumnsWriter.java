package com.alibaba.alink.operator.common.dataproc.format;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class ColumnsWriter extends FormatWriter {

	final int nCols;
	final String[] colNames;
	private FieldParser <?>[] parsers;
	private boolean[] isString;
	private transient Map <String, Integer> keyToFieldIdx;

	public ColumnsWriter(TableSchema schema) {
		this.nCols = schema.getFieldNames().length;
		this.colNames = schema.getFieldNames();
		this.isString = new boolean[colNames.length];
		TypeInformation[] fieldTypes = schema.getFieldTypes();

		this.parsers = new FieldParser[fieldTypes.length];

		for (int i = 0; i < fieldTypes.length; i++) {
			parsers[i] = getFieldParser(fieldTypes[i].getTypeClass());
			isString[i] = fieldTypes[i].equals(Types.STRING);
		}

		keyToFieldIdx = new HashMap <>();
		for (int i = 0; i < colNames.length; i++) {
			keyToFieldIdx.put(colNames[i], i);
		}
	}

	@Override
	public Tuple2 <Boolean, Row> write(Map <String, String> in) {
		boolean success = true;
		Row row = new Row(nCols);
		try {
			for (Map.Entry <String, String> entry : in.entrySet()) {
				Integer idx =  keyToFieldIdx.get(entry.getKey());
				if (null != idx) {
					Tuple2 <Boolean, Object> parsed = parseField(parsers[idx], entry.getValue(), isString[idx]);
					if (parsed.f0) {
						row.setField(idx, parsed.f1);
					}else{
						success = false;
						break;
					}
				}
			}
		} catch (Exception ex) {
			success = false;
		}
		return new Tuple2 <>(success, row);
	}

	static FieldParser <?> getFieldParser(Class typeClazz) {
		Class <? extends FieldParser <?>> parserType = FieldParser.getParserForType(typeClazz);
		if (parserType == null) {
			throw new RuntimeException("No parser available for type '" + typeClazz.getName() + "'.");
		}
		return InstantiationUtil.instantiate(parserType, FieldParser.class);
	}

	static Tuple2 <Boolean, Object> parseField(FieldParser <?> parser, String token, boolean isStringField) {
		if (isStringField) {
			return Tuple2.of(true, token);
		} else {
			if (StringUtils.isNullOrWhitespaceOnly(token)) {
				return Tuple2.of(false, null);
			}
			byte[] bytes = token.getBytes();
			parser.resetErrorStateAndParse(bytes, 0, bytes.length, new byte[] {0}, null);
			FieldParser.ParseErrorState errorState = parser.getErrorState();
			if (errorState != FieldParser.ParseErrorState.NONE) {
				return Tuple2.of(false, null);
			} else {
				return Tuple2.of(true, parser.getLastResult());
			}
		}
	}
}
