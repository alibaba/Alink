package com.alibaba.alink.operator.common.dataproc;

import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.dataproc.CsvToColumnsParams;
import com.alibaba.alink.params.dataproc.JsonToColumnsParams;
import com.alibaba.alink.params.dataproc.KvToColumnsParams;
import com.alibaba.alink.params.dataproc.StringToColumnsParams;
import com.alibaba.alink.params.shared.HasHandleInvalid.HandleInvalidMethod;
import com.alibaba.alink.params.dataproc.vector.VectorToColumnsParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

public class StringToColumnsMappers {

    public static abstract class BaseStringToColumnsMapper extends Mapper {

        private OutputColsHelper outputColsHelper;
        private int idx;
        private String[] fieldNames;
        private TypeInformation[] fieldTypes;
        private transient StringParsers.StringParser parser;
        private transient HandleInvalidMethod handleInvalid;
        private transient boolean hasOpen;

        public BaseStringToColumnsMapper(TableSchema dataSchema, Params params) {
            super(dataSchema, params);
            String selectedColName = this.params.get(JsonToColumnsParams.SELECTED_COL);
            idx = TableUtil.findColIndex(dataSchema.getFieldNames(), selectedColName);
            String schemaStr = this.params.get(JsonToColumnsParams.SCHEMA_STR);
            TableSchema schema = CsvUtil.schemaStr2Schema(schemaStr);
            this.fieldNames = schema.getFieldNames();
            this.fieldTypes = schema.getFieldTypes();
            this.outputColsHelper = new OutputColsHelper(dataSchema, schema.getFieldNames(), schema.getFieldTypes(),
                this.params.get(VectorToColumnsParams.RESERVED_COLS));
        }

        public void open() {
            this.parser = getParser(fieldNames, fieldTypes, params);
            this.handleInvalid = params.get(StringToColumnsParams.HANDLE_INVALID);
        }

        @Override
        public Row map(Row row) {
            if (!hasOpen) {
                open();
                hasOpen = true;
            }
            String text = (String) row.getField(idx);
            Tuple2<Boolean, Row> parsed = parser.parse(text);

            if (!parsed.f0 && handleInvalid == HandleInvalidMethod.ERROR) {
                throw new RuntimeException("Fail to parse \"" + text + "\"");
            }
            return outputColsHelper.getResultRow(row, parsed.f1);
        }

        /**
         * Get the output data schema.
         */
        @Override
        public TableSchema getOutputSchema() {
            return outputColsHelper.getResultSchema();
        }

        abstract protected StringParsers.StringParser getParser(
            String[] fieldNames, TypeInformation[] fieldTypes, Params params);
    }

    public static class CsvToColumnsMapper extends BaseStringToColumnsMapper {
        public CsvToColumnsMapper(TableSchema dataSchema, Params params) {
            super(dataSchema, params);
        }

        @Override
        protected StringParsers.StringParser getParser(String[] fieldNames, TypeInformation[] fieldTypes, Params params) {
            String fieldDelim = params.get(CsvToColumnsParams.FIELD_DELIMITER);
            Character quoteChar = params.get(CsvToColumnsParams.QUOTE_CHAR);
            return new StringParsers.CsvParser(fieldTypes, fieldDelim, quoteChar);
        }
    }

    public static class JsonToColumnsMapper extends BaseStringToColumnsMapper {
        public JsonToColumnsMapper(TableSchema dataSchema, Params params) {
            super(dataSchema, params);
        }

        @Override
        protected StringParsers.StringParser getParser(String[] fieldNames, TypeInformation[] fieldTypes, Params params) {
            return new StringParsers.JsonParser(fieldNames, fieldTypes);
        }
    }

    public static class KvToColumnsMapper extends BaseStringToColumnsMapper {
        public KvToColumnsMapper(TableSchema dataSchema, Params params) {
            super(dataSchema, params);
        }

        @Override
        protected StringParsers.StringParser getParser(String[] fieldNames, TypeInformation[] fieldTypes, Params params) {
            String colDelim = params.get(KvToColumnsParams.COL_DELIMITER);
            String valDelim = params.get(KvToColumnsParams.VAL_DELIMITER);
            return new StringParsers.KvParser(fieldNames, fieldTypes, colDelim, valDelim);
        }
    }
}
