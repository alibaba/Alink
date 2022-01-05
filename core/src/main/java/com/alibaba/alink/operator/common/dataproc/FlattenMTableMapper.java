package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableUtils;
import com.alibaba.alink.common.mapper.FlatMapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.dataproc.FlattenMTableParams;
import com.alibaba.alink.params.shared.HasHandleInvalid.HandleInvalidMethod;

import java.sql.Timestamp;
import java.util.List;

public class FlattenMTableMapper extends FlatMapper {
    private static final long serialVersionUID = 5345439790133072507L;
    private final OutputColsHelper outputColsHelper;
    private final int selectIdx;
    private final String[] outputColNames;
    private final TypeInformation<?>[] outputColTypes;
    private boolean isTypeConvert = false;
    private TypeInformation<?>[] mTableTypes;
    private final HandleInvalidMethod handleInvalidMethod;

    public FlattenMTableMapper(TableSchema dataSchema, Params params) {
        super(dataSchema, params);

        selectIdx = TableUtil.findColIndexWithAssertAndHint(dataSchema,
                params.get(FlattenMTableParams.SELECTED_COL));

        TableSchema schema = CsvUtil.schemaStr2Schema(params.get(FlattenMTableParams.SCHEMA_STR));
        outputColNames = schema.getFieldNames();
        outputColTypes = schema.getFieldTypes();
        String[] reservedColNames = params.contains(FlattenMTableParams.RESERVED_COLS)
                ? params.get(FlattenMTableParams.RESERVED_COLS) : getDataSchema().getFieldNames();
        for (String outputColName : outputColNames) {
            if (TableUtil.findColIndex(reservedColNames, outputColName) > -1) {
                throw new RuntimeException("output table has repeated col names, please check your table schemas.");
            }
        }
        outputColsHelper = new OutputColsHelper(
                dataSchema,
                outputColNames,
                outputColTypes,
                reservedColNames);

        handleInvalidMethod = params.get(FlattenMTableParams.HANDLE_INVALID);
    }

    @Override
    public TableSchema getOutputSchema() {
        return outputColsHelper.getResultSchema();
    }

    @Override
    public void flatMap(Row row, Collector<Row> output) {
        Object[] result = new Object[outputColNames.length];
        Object s = row.getField(selectIdx);
        if (null == s) {
            output.collect(outputColsHelper.getResultRow(row, Row.of(result)));
            return;
        }

        MTable mTable = s instanceof String ? new MTable((String) s) : (MTable) s;
        if (mTable.getNumRow() == 0) {
            return;
        }

        TypeInformation<?>[] tmpTableTypes = mTable.getColTypes();
        String[] tmpTableNames = mTable.getColNames();

        mTableTypes = new TypeInformation<?>[outputColTypes.length];

        for (int i = 0; i < outputColNames.length; ++i) {
            int idx = TableUtil.findColIndex(tmpTableNames, outputColNames[i]);
            mTableTypes[i] = tmpTableTypes[idx];
        }

        for (int i = 0; i < outputColTypes.length; ++i) {
            if (!(outputColTypes[i].equals(mTableTypes[i]))) {
                isTypeConvert = true;
                break;
            }
        }

        List<Object> firstUnEmpty = null;
        for (String name : outputColNames) {
            firstUnEmpty = MTableUtils.getColumn(mTable, name);
        }

        if (firstUnEmpty == null) {
            output.collect(outputColsHelper.getResultRow(row, Row.of(result)));
            return;
        }

        for (int i = 0; i < firstUnEmpty.size(); ++i) {
            for (int j = 0; j < outputColNames.length; ++j) {
                List<Object> element = MTableUtils.getColumn(mTable, outputColNames[j]);
                try {
                    result[j] = typeConvert(element.get(i), j);
                } catch (Throwable ex) {
                    switch (handleInvalidMethod) {
                        case SKIP:
                            result[j] = null;
                            break;
                        case ERROR:
                        default:
                            throw ex;
                    }
                }

            }
            output.collect(outputColsHelper.getResultRow(row, Row.of(result)));
        }

    }

    private Object typeConvert(Object ele, int idx) {
        if (ele == null) {
            return null;
        }
        if (isTypeConvert) {
            if (outputColTypes[idx].equals(mTableTypes[idx])) {
                return ele;
            } else {
                if (ele instanceof Number) {
                    if (Types.DOUBLE.equals(outputColTypes[idx])) {
                        return ((Number) ele).doubleValue();
                    } else if (Types.INT.equals(outputColTypes[idx])) {
                        return ((Number) ele).intValue();
                    } else if (Types.LONG.equals(outputColTypes[idx])) {
                        return ((Number) ele).longValue();
                    } else if (Types.FLOAT.equals(outputColTypes[idx])) {
                        return ((Number) ele).floatValue();
                    } else if (Types.STRING.equals(outputColTypes[idx])) {
                        return ele.toString();
                    } else if (Types.SQL_TIMESTAMP.equals(outputColTypes[idx])) {
                        assert ele instanceof Long;
                        return new Timestamp((long) ele);
                    }
                } else if (ele instanceof String) {
                    if (Types.DOUBLE.equals(outputColTypes[idx])) {
                        return Double.parseDouble((String) ele);
                    } else if (Types.INT.equals(outputColTypes[idx])) {
                        return Integer.parseInt((String) ele);
                    } else if (Types.LONG.equals(outputColTypes[idx])) {
                        return Long.parseLong((String) ele);
                    } else if (Types.FLOAT.equals(outputColTypes[idx])) {
                        return Float.parseFloat((String) ele);
                    } else if (Types.SQL_TIMESTAMP.equals(outputColTypes[idx])) {
                        return Timestamp.valueOf((String) ele);
                    }
                } else if (ele instanceof Timestamp) {
                    if (Types.STRING.equals(outputColTypes[idx])) {
                        return ele.toString();
                    } else if (Types.LONG.equals(outputColTypes[idx])) {
                        return ((Timestamp) ele).getTime();
                    }
                }
            }
        }
        return ele;
    }

}
