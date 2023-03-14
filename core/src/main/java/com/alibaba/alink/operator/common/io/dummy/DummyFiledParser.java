package com.alibaba.alink.operator.common.io.dummy;

import org.apache.flink.types.parser.FieldParser;

public class DummyFiledParser extends FieldParser <String> {

	private boolean quotedStringParsing = false;
	private byte quoteCharacter;

	private String result;

	public void enableQuotedStringParsing(byte quoteCharacter) {
		this.quotedStringParsing = true;
		this.quoteCharacter = quoteCharacter;
	}

	/**
	 * DummyFieldParser doesn't split the input line with field delimiter. It only trims  the last delimiter of the
	 * line, then regards the input line as one whole filed
	 *
	 * @param bytes    The byte array that holds the value.
	 * @param startPos The index where the field starts
	 * @param limit    The limit unto which the byte contents is valid for the parser. The limit is the
	 *                 position one after the last valid byte.
	 * @param delim    The field delimiter character
	 * @param reuse    An optional reusable field to hold the value
	 * @return The index of the next delimiter, if the field was parsed correctly. A value less than 0 otherwise.
	 */
	@Override
	public int parseField(byte[] bytes, int startPos, int limit, byte[] delimiter, String reusable) {

		if (startPos == limit) {
			setErrorState(ParseErrorState.EMPTY_COLUMN);
			this.result = "";
			return limit;
		}

		int i = startPos;

		final int delimLimit = limit - delimiter.length + 1;

		// look for delimiter
		boolean lookForQuote = false;
		while (i < delimLimit) {
			if (!lookForQuote && delimiterNext(bytes, i, delimiter)) {
				i += delimiter.length;
				break;
			} else if (this.quotedStringParsing && bytes[i] == quoteCharacter) {
				lookForQuote = !lookForQuote;
			}
			i++;
		}

		if (i >= delimLimit) {
			this.result = new String(bytes, startPos, limit - startPos, getCharset());
			return limit;
		} else {
			// delimiter found.
			if (i == startPos) {
				setErrorState(ParseErrorState.EMPTY_COLUMN); // mark empty column
			}
			this.result = new String(bytes, startPos, i - startPos, getCharset());
			return i + delimiter.length;
		}
	}

	@Override
	public String createValue() {
		return "";
	}

	@Override
	public String getLastResult() {
		return this.result;
	}
}

