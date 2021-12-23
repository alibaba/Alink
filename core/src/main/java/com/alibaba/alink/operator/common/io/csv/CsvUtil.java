package com.alibaba.alink.operator.common.io.csv;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MTableTypes;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.tensor.TensorTypes;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

/**
 * A utility class for reading and writing csv files.
 */
public class CsvUtil {

	private static final Logger LOG = LoggerFactory.getLogger(CsvUtil.class);

	private static final BiMap <String, TypeInformation <?>> STRING_TYPE_MAP = HashBiMap.create();

	static {
		STRING_TYPE_MAP.put("VARBINARY", BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
		STRING_TYPE_MAP.put("VEC_TYPES_VECTOR", VectorTypes.VECTOR);
		STRING_TYPE_MAP.put("VEC_TYPES_DENSE_VECTOR", VectorTypes.DENSE_VECTOR);
		STRING_TYPE_MAP.put("VEC_TYPES_SPARSE_VECTOR", VectorTypes.SPARSE_VECTOR);
		STRING_TYPE_MAP.put("TENSOR_TYPES_TENSOR", TensorTypes.TENSOR);
		STRING_TYPE_MAP.put("TENSOR_TYPES_BOOL_TENSOR", TensorTypes.BOOL_TENSOR);
		STRING_TYPE_MAP.put("TENSOR_TYPES_BYTE_TENSOR", TensorTypes.BYTE_TENSOR);
		STRING_TYPE_MAP.put("TENSOR_TYPES_DOUBLE_TENSOR", TensorTypes.DOUBLE_TENSOR);
		STRING_TYPE_MAP.put("TENSOR_TYPES_FLOAT_TENSOR", TensorTypes.FLOAT_TENSOR);
		STRING_TYPE_MAP.put("TENSOR_TYPES_INT_TENSOR", TensorTypes.INT_TENSOR);
		STRING_TYPE_MAP.put("TENSOR_TYPES_LONG_TENSOR", TensorTypes.LONG_TENSOR);
		STRING_TYPE_MAP.put("TENSOR_TYPES_STRING_TENSOR", TensorTypes.STRING_TENSOR);
		STRING_TYPE_MAP.put("MTABLE", MTableTypes.M_TABLE);
	}

	/**
	 * Split a string by commas that are not inside parentheses or brackets.
	 *
	 * @param s string
	 * @return split strings.
	 */
	private static String[] robustSpiltByComma(String s) {
		List <String> splits = new ArrayList <>();
		char[] chars = s.toCharArray();
		int start = 0;
		int parenthesesLevel = 0;
		int angleBracketsLevel = 0;
		for (int i = 0; i < chars.length; i += 1) {
			char ch = chars[i];
			if (ch == '(') {
				parenthesesLevel += 1;
			} else if (ch == ')') {
				parenthesesLevel -= 1;
			} else if (ch == '<') {
				angleBracketsLevel += 1;
			} else if (ch == '>') {
				angleBracketsLevel -= 1;
			}
			if (ch == ',' && (parenthesesLevel == 0) && (angleBracketsLevel == 0)) {
				splits.add(new String(chars, start, i - start));
				start = i += 1;
			}
		}
		if (start < s.length()) {
			splits.add(new String(chars, start, s.length() - start));
		}
		return splits.toArray(new String[0]);
	}

	/**
	 * Extract the TableSchema from a string. The format of the string is comma
	 * separated colName-colType pairs, such as "f0 int,f1 bigint,f2 string".
	 *
	 * @param schemaStr The formatted schema string.
	 * @return TableSchema.
	 */
	public static TableSchema schemaStr2Schema(String schemaStr) {
		String[] fields = robustSpiltByComma(schemaStr);
		String[] colNames = new String[fields.length];
		TypeInformation[] colTypes = new TypeInformation[fields.length];
		for (int i = 0; i < colNames.length; i++) {
			String[] kv = fields[i].trim().split("\\s+", 2);
			colNames[i] = kv[0];
			if ((kv[1].toLowerCase()).equals("vector")) {
				kv[1] = "VEC_TYPES_VECTOR";
			}
			if ((kv[1].toLowerCase()).equals("densevector")) {
				kv[1] = "VEC_TYPES_DENSE_VECTOR";
			}
			if ((kv[1].toLowerCase()).equals("sparsevector")) {
				kv[1] = "VEC_TYPES_SPARSE_VECTOR";
			}
			if (STRING_TYPE_MAP.containsKey(kv[1])) {
				colTypes[i] = STRING_TYPE_MAP.get(kv[1]);
			} else {
				if (kv[1].contains("<") && kv[1].contains(">")) {
					colTypes[i] = FlinkTypeConverter.getFlinkType(kv[1]);
				} else {
					colTypes[i] = FlinkTypeConverter.getFlinkType(kv[1].toUpperCase());
				}
			}
		}
		return new TableSchema(colNames, colTypes);
	}

	/**
	 * Transform the TableSchema to a string. The format of the string is comma separated colName-colType pairs,
	 * such as "f0 int,f1 bigint,f2 string".
	 *
	 * @param schema the TableSchema to transform.
	 * @return a string.
	 */
	public static String schema2SchemaStr(TableSchema schema) {
		String[] colNames = schema.getFieldNames();
		TypeInformation[] colTypes = schema.getFieldTypes();

		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < colNames.length; i++) {
			if (i > 0) {
				sbd.append(",");
			}
			String typeName;
			if (STRING_TYPE_MAP.containsValue(colTypes[i])) {
				typeName = STRING_TYPE_MAP.inverse().get(colTypes[i]);
			} else {
				typeName = FlinkTypeConverter.getTypeString(colTypes[i]);
			}
			sbd.append(colNames[i]).append(" ").append(typeName);
		}
		return sbd.toString();
	}

	/**
	 * Parse a text line to a {@link Row}.
	 */
	public static class ParseCsvFunc extends RichFlatMapFunction <Row, Row> {
		private static final long serialVersionUID = -6692520343934146759L;
		private TypeInformation[] colTypes;
		private String fieldDelim;
		private Character quoteChar;
		private boolean skipBlankLine;
		private boolean lenient;
		private transient CsvParser parser;
		private Row emptyRow;

		public ParseCsvFunc(TypeInformation[] colTypes, String fieldDelim, Character quoteChar,
							boolean skipBlankLine, boolean lenient) {
			this.colTypes = colTypes;
			this.fieldDelim = fieldDelim;
			this.quoteChar = quoteChar;
			this.skipBlankLine = skipBlankLine;
			this.lenient = lenient;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			parser = new CsvParser(colTypes, fieldDelim, quoteChar);
			emptyRow = new Row(colTypes.length);
		}

		@Override
		public void flatMap(Row value, Collector <Row> out) throws Exception {
			String line = (String) value.getField(0);
			if (line == null || line.isEmpty()) {
				if (!skipBlankLine) {
					out.collect(emptyRow);
				}
			} else {
				Tuple2 <Boolean, Row> parsed = parser.parse(line);
				if (parsed.f0) {
					out.collect(parsed.f1);
				} else {
					if (!lenient) {
						throw new RuntimeException("Fail to parse line: \"" + line + "\"");
					} else {
						LOG.warn("Fail to parse line: \"" + line + "\"");
					}
				}
			}
		}
	}

	/**
	 * Format a {@link Row} to a text line.
	 */
	public static class FormatCsvFunc extends RichMapFunction <Row, Row> {
		private static final long serialVersionUID = 3828700401508155398L;
		private transient CsvFormatter formater;
		private TypeInformation[] colTypes;
		private String fieldDelim;
		private Character quoteChar;

		public FormatCsvFunc(TypeInformation[] colTypes, String fieldDelim, Character quoteChar) {
			this.colTypes = colTypes;
			this.fieldDelim = fieldDelim;
			this.quoteChar = quoteChar;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.formater = new CsvFormatter(colTypes, fieldDelim, quoteChar);
		}

		@Override
		public Row map(Row row) throws Exception {
			return Row.of(formater.format(row));
		}
	}

	/**
	 * Get column names from a schema string.
	 *
	 * @param schemaStr The formatted schema string.
	 * @return An array of column names.
	 */
	public static String[] getColNames(String schemaStr) {
		return schemaStr2Schema(schemaStr).getFieldNames();
	}

	/**
	 * Get column types from a schema string.
	 *
	 * @param schemaStr The formatted schema string.
	 * @return An array of column types.
	 */
	public static TypeInformation<?>[] getColTypes(String schemaStr) {
		return schemaStr2Schema(schemaStr).getFieldTypes();
	}

	/**
	 * Parse the escape chars and unicode chars from a string, replace them with the chars they represents.
	 * <p>
	 * For example:
	 * <ul>
	 * <li> "\\t" -> '\t'
	 * <li> "\\001" -> '\001'
	 * <li> "\\u0001" -> '\u0001'
	 * </ul>
	 * <p>
	 * The escaped char list: \b, \f, \n, \r, \t, \\, \', \".
	 */
	public static String unEscape(String s) {
		if (s == null) {
			return null;
		}

		if (s.length() == 0) {
			return s;
		}

		StringBuilder sbd = new StringBuilder();

		for (int i = 0; i < s.length(); ) {
			int flag = extractEscape(s, i, sbd);
			if (flag <= 0) {
				sbd.append(s.charAt(i));
				i++;
			} else {
				i += flag;
			}
		}

		return sbd.toString();
	}

	/**
	 * Parse one escape char.
	 *
	 * @param s   The string to parse.
	 * @param pos Starting position of the string.
	 * @param sbd String builder to accept the parsed result.
	 * @return The length of the part of the string that is parsed.
	 */
	private static int extractEscape(String s, int pos, StringBuilder sbd) {
		if (s.charAt(pos) != '\\') {
			return 0;
		}
		pos++;
		if (pos >= s.length()) {
			return 0;
		}
		char c = s.charAt(pos);

		if (c >= '0' && c <= '7') {
			int digit = 1;
			int i;
			for (i = 0; i < 2; i++) {
				if (pos + 1 + i >= s.length()) {
					break;
				}
				if (s.charAt(pos + 1 + i) >= '0' && s.charAt(pos + 1 + i) <= '7') {
					digit++;
				} else {
					break;
				}
			}
			int n = Integer.valueOf(s.substring(pos, pos + digit), 8);
			sbd.append(Character.toChars(n));
			return digit + 1;
		} else if (c == 'u') { // unicode
			pos++;
			int digit = 0;
			for (int i = 0; i < 4; i++) {
				if (pos + i >= s.length()) {
					break;
				}
				char ch = s.charAt(pos + i);
				if ((ch >= '0' && ch <= '9') || ((ch >= 'a' && ch <= 'f')) || ((ch >= 'A' && ch <= 'F'))) {
					digit++;
				} else {
					break;
				}
			}
			if (digit == 0) {
				return 0;
			}
			int n = Integer.valueOf(s.substring(pos, pos + digit), 16);
			sbd.append(Character.toChars(n));
			return digit + 2;
		} else {
			switch (c) {
				case '\\':
					sbd.append('\\');
					return 2;
				case '\'':
					sbd.append('\'');
					return 2;
				case '\"':
					sbd.append('"');
					return 2;
				case 'r':
					sbd.append('\r');
					return 2;
				case 'f':
					sbd.append('\f');
					return 2;
				case 't':
					sbd.append('\t');
					return 2;
				case 'n':
					sbd.append('\n');
					return 2;
				case 'b':
					sbd.append('\b');
					return 2;
				default:
					return 0;
			}
		}
	}

	public static class FlattenCsvFromRow implements MapFunction <Row, String> {
		private static final long serialVersionUID = 4247327927405636850L;
		private final String rowDelimiter;

		public FlattenCsvFromRow(String rowDelimiter) {
			this.rowDelimiter = rowDelimiter;
		}

		@Override
		public String map(Row value) throws Exception {
			StringBuilder builder = new StringBuilder();
			Object o;
			for (int i = 0; i < value.getArity(); i++) {
				if (builder.length() != 0) {
					builder.append(rowDelimiter == null ? "\n" : rowDelimiter);
				}
				if ((o = value.getField(i)) != null) {
					builder.append(o);
				}
			}
			return builder.toString();
		}
	}
}
