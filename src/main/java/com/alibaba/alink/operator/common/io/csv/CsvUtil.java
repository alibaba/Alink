package com.alibaba.alink.operator.common.io.csv;

import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

/**
 * A utility class for reading and writing csv files.
 */
public class CsvUtil {

    /**
     * Extract the TableSchema from a string. The format of the string is comma
     * separated colName-colType pairs, such as "f0 int,f1 bigint,f2 string".
     *
     * @param schemaStr The formatted schema string.
     * @return TableSchema.
     */
    public static TableSchema schemaStr2Schema(String schemaStr) {
        String[] fields = schemaStr.split(",");
        String[] colNames = new String[fields.length];
        TypeInformation[] colTypes = new TypeInformation[fields.length];
        for (int i = 0; i < colNames.length; i++) {
            String[] kv = fields[i].trim().split("\\s+");
            colNames[i] = kv[0];
            if (colNames[i].equalsIgnoreCase("varbinary")) {
                colTypes[i] = BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
            } else {
                colTypes[i] = FlinkTypeConverter.getFlinkType(kv[1].toUpperCase());
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
            if (colTypes[i].equals(BYTE_PRIMITIVE_ARRAY_TYPE_INFO)) {
                typeName = "varbinary";
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
    public static class ParseCsvFunc extends RichFlatMapFunction<Row, Row> {
        private TypeInformation[] colTypes;
        private String fieldDelim;
        private Character quoteChar;
        private boolean skipBlankLine;
        private transient CsvParser parser;
        private Row emptyRow;

        public ParseCsvFunc(TypeInformation[] colTypes, String fieldDelim, Character quoteChar, boolean skipBlankLine) {
            this.colTypes = colTypes;
            this.fieldDelim = fieldDelim;
            this.quoteChar = quoteChar;
            this.skipBlankLine = skipBlankLine;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            parser = new CsvParser(colTypes, fieldDelim, quoteChar);
            emptyRow = new Row(colTypes.length);
        }

        @Override
        public void flatMap(Row value, Collector<Row> out) throws Exception {
            String line = (String) value.getField(0);
            if (line == null || line.isEmpty()) {
                if (!skipBlankLine) {
                    out.collect(emptyRow);
                }
            } else {
                out.collect(parser.parse(line));
            }
        }
    }

    /**
     * Format a {@link Row} to a text line.
     */
    public static class FormatCsvFunc extends RichMapFunction<Row, Row> {
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
    public static TypeInformation[] getColTypes(String schemaStr) {
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
}
