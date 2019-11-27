package com.alibaba.alink.operator.common.io.csv;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

/**
 * CsvParser parse a text line to a {@link Row}.
 */
public class CsvParser {
    private final String fieldDelim;
    private final int lenFieldDelim;
    private Character quoteChar;
    private String quoteString;
    private String escapedQuote;
    private Row reused;
    private boolean enableQuote;
    private FieldParser<?>[] parsers;
    private boolean[] isString;

    /**
     * The Constructor.
     *
     * @param types      Column types.
     * @param fieldDelim Field delimiter in the text line.
     * @param quoteChar  Quoting character. Contents between a pair of quoting chars are treated as a field, even if
     *                   contains field delimiters. Two consecutive quoting chars represents a real quoting char.
     */
    public CsvParser(TypeInformation[] types, String fieldDelim, @Nullable Character quoteChar) {
        this.fieldDelim = fieldDelim;
        this.lenFieldDelim = this.fieldDelim.length();
        this.quoteChar = quoteChar;
        this.reused = new Row(types.length);
        this.enableQuote = quoteChar != null;
        this.parsers = new FieldParser[types.length];
        this.isString = new boolean[types.length];

        if (enableQuote) {
            this.quoteString = quoteChar.toString();
            this.escapedQuote = this.quoteString + this.quoteString;
        }

        for (int i = 0; i < types.length; i++) {
            Class typeClazz = types[i].getTypeClass();
            Class<? extends FieldParser<?>> parserType = FieldParser.getParserForType(typeClazz);
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
     * @return The parsed result.
     */
    public Row parse(String line) {
        for (int i = 0; i < reused.getArity(); i++) {
            reused.setField(i, null);
        }
        if (line == null || line.isEmpty()) {
            return reused;
        }
        int startPos = 0;
        final int limit = line.length();
        for (int i = 0; i < reused.getArity(); i++) {
            if (startPos >= limit) {
                break;
            }
            boolean isStringCol = isString[i];
            int delimPos = findNextDelimPos(line, startPos, limit, isStringCol);
            if (delimPos < 0) {
                delimPos = limit;
            }
            String token = line.substring(startPos, delimPos);
            if (!token.isEmpty()) {
                reused.setField(i, parseField(parsers[i], token, isStringCol));
            }
            startPos = delimPos + this.lenFieldDelim;
        }
        return reused;
    }

    private int findNextDelimPos(String line, int startPos, int limit, boolean isStringCol) {
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
            throw new RuntimeException("Unterminated quote.");
        }
        return line.indexOf(fieldDelim, pos + 1);
    }

    private Object parseField(FieldParser<?> parser, String token, boolean isStringField) {
        if (isStringField) {
            if (!enableQuote || token.charAt(0) != quoteChar) {
                return token;
            }
            Preconditions.checkArgument(token.endsWith(quoteChar.toString()),
                "String not end with quote: " + String.format("\"%s\"", token));
            String content = token.substring(1, token.length() - 1);
            return content.replace(escapedQuote, quoteString);
        } else {
            if (StringUtils.isNullOrWhitespaceOnly(token)) {
                return null;
            }
            byte[] bytes = token.getBytes();
            parser.resetErrorStateAndParse(bytes, 0, bytes.length, fieldDelim.getBytes(), null);
            FieldParser.ParseErrorState errorState = parser.getErrorState();
            if (errorState != FieldParser.ParseErrorState.NONE) {
                throw new RuntimeException("Fail to parse token: " + String.format("\"%s\"", token));
            }
            return parser.getLastResult();
        }
    }
}
