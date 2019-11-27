package com.alibaba.alink.operator.common.io.csv;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

/**
 * CsvFormatter formats a {@link Row} to a text line.
 */
public class CsvFormatter {
    private TypeInformation[] types;
    private String fieldDelim;
    private String quoteChar;
    private String escapeChar;

    /**
     * The Constructor.
     *
     * @param types      Column types.
     * @param fieldDelim Field delimiter in the text line.
     * @param quoteChar  Quoting character. Used to quote a string field if it has field delimiters.
     */
    public CsvFormatter(TypeInformation[] types, String fieldDelim, @Nullable Character quoteChar) {
        this.types = types;
        this.fieldDelim = fieldDelim;
        if (quoteChar != null) {
            this.quoteChar = quoteChar.toString();
            this.escapeChar = this.quoteChar;
        }
    }

    /**
     * Format a row to a text line.
     *
     * @param row The row to format.
     * @return A text line.
     */
    public String format(Row row) {
        StringBuilder sbd = new StringBuilder();
        for (int i = 0; i < row.getArity(); i++) {
            if (i > 0) {
                sbd.append(fieldDelim);
            }
            Object v = row.getField(i);
            if (v == null) {
                continue;
            }
            if (quoteChar != null && types[i].equals(Types.STRING)) {
                String str = (String) v;
                if (str.isEmpty() || str.contains(fieldDelim) || str.contains(quoteChar)) {
                    sbd.append(quoteChar);
                    sbd.append(str.replace(quoteChar, escapeChar + quoteChar));
                    sbd.append(quoteChar);
                } else {
                    sbd.append(v.toString());
                }
            } else {
                sbd.append(v.toString());
            }
        }
        return sbd.toString();
    }
}
