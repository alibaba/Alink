package com.alibaba.alink.operator.common.dataproc.format;

import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.dataproc.format.*;
import com.alibaba.alink.params.io.HasSchemaStr;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class FormatTransMapper extends Mapper {

    private OutputColsHelper outputColsHelper;
    private HasHandleInvalidDefaultAsError.HandleInvalid handleInvalid;
    private transient FormatReader formatReader;
    private transient FormatWriter formatWriter;

    /**
     * Constructor.
     *
     * @param dataSchema the dataSchema.
     * @param params     the params.
     */
    public FormatTransMapper(TableSchema dataSchema, Params params) {
        super(dataSchema, params);

        Tuple2<FormatReader, String[]> t2From = initFormatReader(dataSchema, params);
        this.formatReader = t2From.f0;
        String[] fromColNames = t2From.f1;

        Tuple3<FormatWriter, String[], TypeInformation[]> t3To = initFormatWriter(params, fromColNames);
        formatWriter = t3To.f0;
        String[] outputColNames = t3To.f1;
        TypeInformation[] outputColTypes = t3To.f2;

        this.handleInvalid = params.get(HasHandleInvalidDefaultAsError.HANDLE_INVALID);
        this.outputColsHelper = new OutputColsHelper(dataSchema, outputColNames, outputColTypes,
            this.params.get(HasReservedColsDefaultAsNull.RESERVED_COLS));
    }

    @Override
    public void open() {
        Tuple2<FormatReader, String[]> t2From = initFormatReader(super.getDataSchema(), params);
        this.formatReader = t2From.f0;
        String[] fromColNames = t2From.f1;

        Tuple3<FormatWriter, String[], TypeInformation[]> t3To = initFormatWriter(params, fromColNames);
        formatWriter = t3To.f0;
    }

    public static Tuple2<FormatReader, String[]> initFormatReader(TableSchema dataSchema, Params params) {
        FormatReader formatReader;
        String[] fromColNames;

        FormatType fromFormat = params.get(FormatTransParams.FROM_FORMAT);
        switch (fromFormat) {
            case KV:
                String kvColName = params.get(FromKvParams.KV_COL);
                int kvColIndex = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), kvColName);
                formatReader = new KvReader(
                    kvColIndex,
                    params.get(FromKvParams.KV_COL_DELIMITER),
                    params.get(FromKvParams.KV_VAL_DELIMITER)
                );
                fromColNames = null;
                break;
            case CSV:
                String csvColName = params.get(FromCsvParams.CSV_COL);
                int csvColIndex = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), csvColName);
                TableSchema fromCsvSchema = CsvUtil.schemaStr2Schema(params.get(FromCsvParams.SCHEMA_STR));
                formatReader = new CsvReader(
                    csvColIndex,
                    fromCsvSchema,
                    params.get(FromCsvParams.CSV_FIELD_DELIMITER),
                    params.get(FromCsvParams.QUOTE_CHAR)
                );
                fromColNames = fromCsvSchema.getFieldNames();
                break;
            case VECTOR:
                String vectorColName = params.get(FromVectorParams.VECTOR_COL);
                int vectorColIndex = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(),
                    vectorColName);
                if (params.contains(HasSchemaStr.SCHEMA_STR)) {
                    formatReader = new VectorReader(
                        vectorColIndex,
                        CsvUtil.schemaStr2Schema(params.get(HasSchemaStr.SCHEMA_STR))
                    );
                } else {
                    formatReader = new VectorReader(vectorColIndex, null);
                }
                fromColNames = null;
                break;
            case JSON:
                String jsonColName = params.get(FromJsonParams.JSON_COL);
                int jsonColIndex = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), jsonColName);
                formatReader = new JsonReader(jsonColIndex);
                fromColNames = null;
                break;
            case COLUMNS:
                fromColNames = params.get(FromColumnsParams.SELECTED_COLS);
                if (null == fromColNames) {
                    fromColNames = dataSchema.getFieldNames();
                }
                int[] colIndices = TableUtil.findColIndicesWithAssertAndHint(dataSchema.getFieldNames(), fromColNames);
                formatReader = new ColumnsReader(colIndices, fromColNames);
                break;
            default:
                throw new IllegalArgumentException("Can not translate this type : " + fromFormat);
        }

        return new Tuple2<>(formatReader, fromColNames);
    }

    public static Tuple3<FormatWriter, String[], TypeInformation[]> initFormatWriter(Params params,
                                                                                     String[] fromColNames) {
        FormatType toFormat = params.get(FormatTransParams.TO_FORMAT);
        FormatWriter formatWriter;
        String[] outputColNames;
        TypeInformation[] outputColTypes;

        switch (toFormat) {
            case COLUMNS:
                TableSchema schema = CsvUtil.schemaStr2Schema(params.get(ToColumnsParams.SCHEMA_STR));
                formatWriter = new ColumnsWriter(schema);
                outputColNames = schema.getFieldNames();
                outputColTypes = schema.getFieldTypes();
                break;
            case JSON:
                formatWriter = new JsonWriter();
                outputColNames = new String[]{params.get(ToJsonParams.JSON_COL)};
                outputColTypes = new TypeInformation[]{Types.STRING};
                break;
            case KV:
                formatWriter = new KvWriter(
                    params.get(ToKvParams.KV_COL_DELIMITER),
                    params.get(ToKvParams.KV_VAL_DELIMITER)
                );
                outputColNames = new String[]{params.get(ToKvParams.KV_COL)};
                outputColTypes = new TypeInformation[]{Types.STRING};
                break;
            case CSV:
                formatWriter = new CsvWriter(
                    CsvUtil.schemaStr2Schema(params.get(ToCsvParams.SCHEMA_STR)),
                    params.get(ToCsvParams.CSV_FIELD_DELIMITER),
                    params.get(ToCsvParams.QUOTE_CHAR)
                );
                outputColNames = new String[]{params.get(ToCsvParams.CSV_COL)};
                outputColTypes = new TypeInformation[]{Types.STRING};
                break;
            case VECTOR:
                formatWriter = new VectorWriter(
                    params.get(ToVectorParams.VECTOR_SIZE),
                    fromColNames
                );
                outputColNames = new String[]{params.get(ToVectorParams.VECTOR_COL)};
                outputColTypes = new TypeInformation[]{Types.STRING};
                break;
            default:
                throw new IllegalArgumentException("Can not translate to this type : " + toFormat);
        }

        return new Tuple3<>(formatWriter, outputColNames, outputColTypes);

    }

    /**
     * The operation function to transform vector to table columns.
     *
     * @param row the input Row type data
     * @return the output row.
     */
    @Override
    public Row map(Row row) {
        if (null == row) {
            return null;
        }
        Map<String, String> bufMap = new HashMap<>();
        boolean success = formatReader.read(row, bufMap);
        if (!success && handleInvalid.equals(HasHandleInvalidDefaultAsError.HandleInvalid.ERROR)) {
            throw new RuntimeException("Fail to read: " + row);
        }
        Tuple2<Boolean, Row> result = formatWriter.write(bufMap);
        if (!result.f0 && handleInvalid.equals(HasHandleInvalidDefaultAsError.HandleInvalid.ERROR)) {
            throw new RuntimeException("Fail to write: " + JsonConverter.toJson(bufMap));
        }
        if (params.get(FormatTransParams.FROM_FORMAT).equals(FormatType.VECTOR) &&
            params.get(FormatTransParams.TO_FORMAT).equals(FormatType.COLUMNS)) {
            int length = result.f1.getArity();
            for (int i = 0; i < length; i++) {
                if (result.f1.getField(i) == null) {
                    result.f1.setField(i, 0.0);
                }
            }
        }
        return outputColsHelper.getResultRow(row, result.f1);
    }

    /**
     * Get the output data schema.
     */
    @Override
    public TableSchema getOutputSchema() {
        return outputColsHelper.getResultSchema();
    }
}
