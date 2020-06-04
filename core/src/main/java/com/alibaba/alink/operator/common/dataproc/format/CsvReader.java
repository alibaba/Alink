package com.alibaba.alink.operator.common.dataproc.format;

import com.alibaba.alink.operator.common.io.csv.CsvParser;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.Map;

public class CsvReader extends FormatReader {

    private CsvParser parser;
    private int csvColIndex;
    private String[] colNames;

    public CsvReader(int csvColIndex, TableSchema schema, String fieldDelim, Character quoteChar) {
        this.parser = new CsvParser(schema.getFieldTypes(), fieldDelim, quoteChar);

        this.csvColIndex = csvColIndex;
        this.colNames = schema.getFieldNames();
    }

    @Override
    boolean read(Row row, Map<String, String> out) {
        String line = (String) row.getField(csvColIndex);
        Tuple2<Boolean, Row> parsed = parser.parse(line);

        for (int i = 0; i < parsed.f1.getArity(); i++) {
            if (parsed.f1.getField(i) != null) {
                out.put(colNames[i], String.valueOf(parsed.f1.getField(i)));
            } else {
                out.put(colNames[i], null);
            }
        }
        return parsed.f0;
    }
}
