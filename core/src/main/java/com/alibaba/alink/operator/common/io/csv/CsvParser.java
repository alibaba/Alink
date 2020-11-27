package com.alibaba.alink.operator.common.io.csv;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * CsvParser parse a text line to a {@link Row}.
 */
public class CsvParser implements Serializable {
	private static final long serialVersionUID = 5688534307667798259L;
	private final String fieldDelim;
	private final int lenFieldDelim;
	private Character quoteChar;
	private String quoteString;
	private String escapedQuote;
	private boolean enableQuote;
	private FieldParser <?>[] parsers;
	private TypeInformation <?>[] types;
	private boolean[] isString;
	private boolean[] isVector;

	/**
	 * The Constructor.
	 *
	 * @param types      Column types.
	 * @param fieldDelim Field delimiter in the text line.
	 * @param quoteChar  Quoting character. Contents between a pair of quoting chars are treated as a field, even if
	 *                   contains field delimiters. Two consecutive quoting chars represents a real quoting char.
	 */
	public CsvParser(TypeInformation<?>[] types, String fieldDelim, @Nullable Character quoteChar) {
		this.types = types;
		this.fieldDelim = fieldDelim;
		this.lenFieldDelim = this.fieldDelim.length();
		this.quoteChar = quoteChar;
		this.enableQuote = quoteChar != null;
		this.parsers = new FieldParser[types.length];
		this.isString = new boolean[types.length];
		this.isVector = new boolean[types.length];

		if (enableQuote) {
			this.quoteString = quoteChar.toString();
			this.escapedQuote = this.quoteString + this.quoteString;
		}

		for (int i = 0; i < types.length; i++) {
			Class<?> typeClazz = types[i].getTypeClass();
			if (typeClazz.equals(Vector.class) || typeClazz.equals(DenseVector.class) || typeClazz.equals(
				SparseVector.class)) {
				typeClazz = String.class;
				isVector[i] = true;
			}
			Class <? extends FieldParser <?>> parserType = FieldParser.getParserForType(typeClazz);
			if (parserType == null) {
				throw new RuntimeException("No parser available for type '" + typeClazz.getName() + "'.");
			}
			parsers[i] = InstantiationUtil.instantiate(parserType, FieldParser.class);
			isString[i] = types[i].equals(Types.STRING);
		}
	}

	/**
	 * Parse a text line.
	 *
	 * @param line The text line to parse.
	 * @return The parsed result. Tuple.f0 indicates whether all fields are correctly parsed.
	 */
	public Tuple2 <Boolean, Row> parse(String line) {
		Row output = new Row(this.parsers.length);
		for (int i = 0; i < output.getArity(); i++) {
			output.setField(i, null);
		}
		if (line == null || line.isEmpty()) {
			return Tuple2.of(false, output);
		}
		int startPos = 0;
		boolean succ = true;
		final int limit = line.length();
		for (int i = 0; i < output.getArity(); i++) {
			if (startPos > limit) {
				succ = false;
				break;
			}
			boolean isStringCol = isString[i];
			int delimPos = findNextDelimPos(line, startPos, limit, isStringCol);
			if (delimPos < 0) {
				delimPos = limit;
			}
			String token = line.substring(startPos, delimPos);
			if (!token.isEmpty()) {
				Tuple2 <Boolean, Object> parsed = parseField(parsers[i], token, isStringCol);
				if (!parsed.f0) {
					succ = false;
				}
				if (isVector[i]) {
					parsed = postProcess(parsed, types[i]);
				}
				output.setField(i, parsed.f1);
			}
			startPos = delimPos + this.lenFieldDelim;
		}
		return Tuple2.of(succ, output);
	}

	private Tuple2 <Boolean, Object> postProcess(Tuple2 <Boolean, Object> record, TypeInformation <?> type) {
		if (record.f1 == null) {
			return record;
		}
		if (type.equals(VectorTypes.VECTOR)) {
			record.f1 = VectorUtil.getVector(record.f1);
		} else if (type.equals(VectorTypes.DENSE_VECTOR)) {
			record.f1 = VectorUtil.getDenseVector(record.f1);

		} else if (type.equals(VectorTypes.SPARSE_VECTOR)) {
			record.f1 = VectorUtil.getSparseVector(record.f1);
		}
		return record;
	}

	private int findNextDelimPos(String line, int startPos, int limit, boolean isStringCol) {
		if (startPos >= limit) {
			return -1;
		}
		if (!enableQuote || !isStringCol) {
			return line.indexOf(fieldDelim, startPos);
		}
		boolean startsWithQuote = line.charAt(startPos) == quoteChar;
		if (!startsWithQuote) {
			return line.indexOf(fieldDelim, startPos);
		}
		int pos = startPos + 1;
		boolean escaped = false;
		while (pos < limit) {
			char c = line.charAt(pos);
			if (c == quoteChar) {
				if (!escaped) { // c is either terminating quote or an escape
					if (pos + 1 < limit && line.charAt(pos + 1) == quoteChar) {
						escaped = true;
					} else { // c is terminating quote
						break;
					}
				} else {
					escaped = false;
				}
			}
			pos++;
		}
		if (pos >= limit) {
			return -1;
		}
		return line.indexOf(fieldDelim, pos + 1);
	}

	private Tuple2 <Boolean, Object> parseField(FieldParser <?> parser, String token, boolean isStringField) {
		if (isStringField) {
			if (!enableQuote || token.charAt(0) != quoteChar) {
				return Tuple2.of(true, token);
			}
			String content;
			if (token.endsWith(quoteChar.toString())) {
				content = token.substring(1, token.length() - 1);
			} else {
				content = token.substring(1, token.length());
			}
			return Tuple2.of(true, content.replace(escapedQuote, quoteString));
		} else {
			if (StringUtils.isNullOrWhitespaceOnly(token)) {
				return Tuple2.of(true, null);
			}
			if (token.equals("\"\"")) {
				return Tuple2.of(true, null); // spark output's null value as ""
			}
			byte[] bytes = token.getBytes();
			parser.resetErrorStateAndParse(bytes, 0, bytes.length, fieldDelim.getBytes(), null);
			FieldParser.ParseErrorState errorState = parser.getErrorState();
			if (errorState != FieldParser.ParseErrorState.NONE) {
				return Tuple2.of(false, null);
			} else {
				return Tuple2.of(true, parser.getLastResult());
			}
		}
	}
}
