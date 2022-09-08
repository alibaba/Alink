package com.alibaba.alink.operator.common.io.csv;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.exceptions.AkParseErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class for reading and writing csv files.
 */
public class CsvUtil {

	private static final Logger LOG = LoggerFactory.getLogger(CsvUtil.class);

	/**
	 * Parse a text line to a {@link Row}.
	 */
	public static class ParseCsvFunc extends RichFlatMapFunction <Row, Row> {
		private static final long serialVersionUID = -6692520343934146759L;
		private final TypeInformation <?>[] colTypes;
		private final String fieldDelim;
		private final Character quoteChar;
		private final boolean skipBlankLine;
		private final boolean lenient;
		private transient CsvParser parser;
		private Row emptyRow;

		public ParseCsvFunc(TypeInformation <?>[] colTypes, String fieldDelim, Character quoteChar,
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
					out.collect(new Row(colTypes.length));
				}
			} else {
				Tuple2 <Boolean, Row> parsed = parser.parse(line);
				if (parsed.f0) {
					out.collect(parsed.f1);
				} else {
					if (!lenient) {
						throw new AkParseErrorException("Fail to parse line: \"" + line + "\"");
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
		private transient CsvFormatter formatter;
		private final TypeInformation <?>[] colTypes;
		private final String fieldDelim;
		private final Character quoteChar;

		public FormatCsvFunc(TypeInformation <?>[] colTypes, String fieldDelim, Character quoteChar) {
			this.colTypes = colTypes;
			this.fieldDelim = fieldDelim;
			this.quoteChar = quoteChar;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			this.formatter = new CsvFormatter(colTypes, fieldDelim, quoteChar);
		}

		@Override
		public Row map(Row row) throws Exception {
			return Row.of(formatter.format(row));
		}
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
