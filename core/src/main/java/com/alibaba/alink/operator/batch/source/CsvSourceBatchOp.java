package com.alibaba.alink.operator.batch.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.operator.common.io.csv.GenericCsvInputFormat;
import com.alibaba.alink.operator.common.io.reader.HttpFileSplitReader;
import com.alibaba.alink.params.io.CsvSourceParams;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Data source of a CSV (Comma Separated Values) file.
 * <p>
 * The file can reside in places including:
 * <p><ul>
 * <li> local file system
 * <li> hdfs
 * <li> http
 * </ul></p>
 */

@IoOpAnnotation(name = "csv", ioType = IOType.SourceBatch)
public final class CsvSourceBatchOp extends BaseSourceBatchOp<CsvSourceBatchOp>
    implements CsvSourceParams<CsvSourceBatchOp> {

    public CsvSourceBatchOp() {
        this(new Params());
    }

    public CsvSourceBatchOp(Params params) {
        super(AnnotationUtils.annotatedName(CsvSourceBatchOp.class), params);
    }

    public CsvSourceBatchOp(String filePath, String schemaStr) {
        this(new Params()
            .set(FILE_PATH, filePath)
            .set(SCHEMA_STR, schemaStr)
        );
    }

    public CsvSourceBatchOp(String filePath, TableSchema schema) {
        this(new Params()
            .set(FILE_PATH, filePath)
            .set(SCHEMA_STR, CsvUtil.schema2SchemaStr(schema))
        );
    }

    public CsvSourceBatchOp(String filePath, String[] colNames, TypeInformation<?>[] colTypes,
                            String fieldDelim, String rowDelim) {
        this(new Params()
            .set(FILE_PATH, filePath)
            .set(SCHEMA_STR, CsvUtil.schema2SchemaStr(new TableSchema(colNames, colTypes)))
            .set(FIELD_DELIMITER, fieldDelim)
            .set(ROW_DELIMITER, rowDelim)
        );
    }

    @Override
    public Table initializeDataSource() {
        final String filePath = getFilePath();
        final String schemaStr = getSchemaStr();
        final String fieldDelim = getFieldDelimiter();
        final String rowDelim = getRowDelimiter();
        final Character quoteChar = getQuoteChar();
        final boolean skipBlankLine = getSkipBlankLine();

        final String[] colNames = CsvUtil.getColNames(schemaStr);
        final TypeInformation[] colTypes = CsvUtil.getColTypes(schemaStr);

        boolean ignoreFirstLine = getIgnoreFirstLine();
        String protocol = "";

        try {
            URL url = new URL(filePath);
            protocol = url.getProtocol();
        } catch (MalformedURLException ignored) {
        }

        DataSet<Row> rows;
        ExecutionEnvironment execEnv = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment();
        TableSchema dummySchema = new TableSchema(new String[]{"f1"}, new TypeInformation[]{Types.STRING});

        if (protocol.equalsIgnoreCase("http") || protocol.equalsIgnoreCase("https")) {
            HttpFileSplitReader reader = new HttpFileSplitReader(filePath);
            rows = execEnv
                .createInput(new GenericCsvInputFormat(reader, dummySchema.getFieldTypes(), rowDelim, rowDelim, ignoreFirstLine),
                    new RowTypeInfo(dummySchema.getFieldTypes(), dummySchema.getFieldNames()))
                .name("http_csv_source");
        } else {
            RowCsvInputFormat inputFormat = new RowCsvInputFormat(
                new Path(filePath), dummySchema.getFieldTypes(), rowDelim, rowDelim, new int[]{0}, true);
            inputFormat.setSkipFirstLineAsHeader(ignoreFirstLine);
            rows = execEnv.createInput(inputFormat).name("csv_source");
        }

        rows = rows.flatMap(new CsvUtil.ParseCsvFunc(colTypes, fieldDelim, quoteChar, skipBlankLine));

        return DataSetConversionUtil.toTable(getMLEnvironmentId(), rows, colNames, colTypes);
    }
}
