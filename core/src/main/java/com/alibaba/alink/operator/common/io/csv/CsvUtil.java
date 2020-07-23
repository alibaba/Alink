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

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

/**
 * A utility class for reading and writing csv files.
 */
public class CsvUtil {

    private static final Logger LOG = LoggerFactory.getLogger(CsvUtil.class);

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

            if (kv[1].equalsIgnoreCase("VARBINARY")) {
                colTypes[i] = BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
            } else if (kv[1].equalsIgnoreCase("VEC_TYPES_VECTOR")) {
                colTypes[i] = VectorTypes.VECTOR;
            } else if (kv[1].equalsIgnoreCase("VEC_TYPES_DENSE_VECTOR")) {
                colTypes[i] = VectorTypes.DENSE_VECTOR;
            } else if (kv[1].equalsIgnoreCase("VEC_TYPES_SPARSE_VECTOR")) {
                colTypes[i] = VectorTypes.SPARSE_VECTOR;
            } else {
                if(kv[1].contains("<") && kv[1].contains(">")) {
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
            if (colTypes[i].equals(BYTE_PRIMITIVE_ARRAY_TYPE_INFO)) {
                typeName = "VARBINARY";
            } else if (colTypes[i].equals(VectorTypes.VECTOR)) {
                typeName = "VEC_TYPES_VECTOR";
            } else if (colTypes[i].equals(VectorTypes.DENSE_VECTOR)) {
                typeName = "VEC_TYPES_DENSE_VECTOR";
            } else if (colTypes[i].equals(VectorTypes.SPARSE_VECTOR)) {
                typeName = "VEC_TYPES_SPARSE_VECTOR";
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
        public void flatMap(Row value, Collector<Row> out) throws Exception {
            String line = (String) value.getField(0);
            if (line == null || line.isEmpty()) {
                if (!skipBlankLine) {
                    out.collect(emptyRow);
                }
            } else {
                Tuple2<Boolean, Row> parsed = parser.parse(line);
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

    public static class FlattenCsvFromRow implements MapFunction<Row, String> {
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
