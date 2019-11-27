package com.alibaba.alink.common.utils;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

/**
 * Utility class for the operations on Table, such as the operations on column name, the operations on column type and
 * so on.
 */
public class TableUtil {
    /**
     * Return a lower temp table name in lower case and use "-" as connector character.
     *
     * @return tableName
     */
    public static synchronized String getTempTableName() {
        return ("temp_" + UUID.randomUUID().toString().replaceAll("-", "_"))
            .toLowerCase();
    }

    /**
     * Find the index of <code>targetCol</code> in string array <code>tableCols</code>. It will ignore the case of the
     * tableCols.
     *
     * @param tableCols a string array among which to find the targetCol.
     * @param targetCol the targetCol to find.
     * @return the index of the targetCol, if not found, returns -1.
     */
    public static int findColIndex(String[] tableCols, String targetCol) {
        Preconditions.checkNotNull(targetCol, "targetCol is null!");
        for (int i = 0; i < tableCols.length; i++) {
            if (targetCol.equalsIgnoreCase(tableCols[i])) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Find the indices of <code>targetCols</code> in string array <code>tableCols</code>. If
     * <code>targetCols</code> is
     * null, it will be replaced by the <code>tableCols</code>
     *
     * @param tableCols  a string array among which to find the targetCols.
     * @param targetCols the targetCols to find.
     * @return the indices of the targetCols.
     */
    public static int[] findColIndices(String[] tableCols, String[] targetCols) {
        if (targetCols == null) {
            int[] indices = new int[tableCols.length];
            for (int i = 0; i < tableCols.length; i++) {
                indices[i] = i;
            }
            return indices;
        }
        int[] indices = new int[targetCols.length];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = findColIndex(tableCols, targetCols[i]);
        }
        return indices;
    }

    /**
     * Find the index of <code>targetCol</code> from the <code>tableSchema</code>.
     *
     * @param tableSchema the TableSchema among which to find the targetCol.
     * @param targetCol   the targetCols to find.
     * @return the index of the targetCol.
     */
    public static int findColIndex(TableSchema tableSchema, String targetCol) {
        return findColIndex(tableSchema.getFieldNames(), targetCol);
    }

    /**
     * Find the indices of <code>targetCols</code> from the <code>tableSchema</code>.
     *
     * @param tableSchema the TableSchema among which to find the targetCols.
     * @param targetCols  the targetCols to find.
     * @return the indices of the targetCols.
     */
    public static int[] findColIndices(TableSchema tableSchema, String[] targetCols) {
        return findColIndices(tableSchema.getFieldNames(), targetCols);
    }

    /**
     * Find the types of the <code>targetCols</code>. If the targetCol not exist, return null.
     *
     * @param tableSchema TableSchema.
     * @param targetCols  the targetCols to find.
     * @return the corresponding types.
     */
    public static TypeInformation[] findColTypes(TableSchema tableSchema, String[] targetCols) {
        TypeInformation[] types = new TypeInformation[targetCols.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = findColType(tableSchema, targetCols[i]);
        }
        return types;
    }

    /**
     * Find the type of the <code>targetCol</code>. If the targetCol not exist, return null.
     *
     * @param tableSchema TableSchema
     * @param targetCol   the targetCol to find.
     * @return the corresponding type.
     */
    public static TypeInformation findColType(TableSchema tableSchema, String targetCol) {
        int index = findColIndex(tableSchema.getFieldNames(), targetCol);

        return index == -1 ? null : tableSchema.getFieldTypes()[index];
    }

    /**
     * Determine whether it is number type, number type includes double, long, byte, int, float and short.
     *
     * @param dataType the dataType to determine.
     * @return whether it is number type
     */
    public static boolean isNumber(TypeInformation dataType) {
        return Types.DOUBLE.equals(dataType)
            || Types.LONG.equals(dataType)
            || Types.BYTE.equals(dataType)
            || Types.INT.equals(dataType)
            || Types.FLOAT.equals(dataType)
            || Types.SHORT.equals(dataType)
            || Types.BIG_DEC.equals(dataType);
    }

    /**
     * Determine whether it is a string type.
     *
     * @param dataType the dataType to determine.
     * @return whether it is string type
     */
    public static boolean isString(TypeInformation dataType) {
        return Types.STRING == dataType;
    }

    /**
     * Determine whether it is a vector type.
     *
     * @param dataType the dataType to determine.
     * @return whether it is vector type
     */
    public static boolean isVector(TypeInformation dataType) {
        return VectorTypes.VECTOR.equals(dataType)
            || VectorTypes.DENSE_VECTOR.equals(dataType)
            || VectorTypes.SPARSE_VECTOR.equals(dataType)
            || Types.STRING == dataType
            ;
    }

    /**
     * Check whether <code>selectedCols</code> exist or not, if not exist, throw exception.
     *
     * @param tableCols    a string array among which to find the target selectedCols.
     * @param selectedCols the selectedCols to assert.
     */
    public static void assertSelectedColExist(String[] tableCols, String... selectedCols) {
        if (null != selectedCols) {
            for (String selectedCol : selectedCols) {
                if (null != selectedCol) {
                    if (-1 == findColIndex(tableCols, selectedCol)) {
                        throw new IllegalArgumentException(" col is not exist " + selectedCol);
                    }
                }
            }
        }
    }

    /**
     * Check whether colTypes of the <code>selectedCols</code> is numerical, if not, throw exception.
     *
     * @param tableSchema  TableSchema
     * @param selectedCols the selectedCols to assert.
     */
    public static void assertNumericalCols(TableSchema tableSchema, String... selectedCols) {
        if (selectedCols != null && selectedCols.length != 0) {
            for (String selectedCol : selectedCols) {
                if (null != selectedCol) {
                    if (!isNumber(findColType(tableSchema, selectedCol))) {
                        throw new IllegalArgumentException("col type must be number " + selectedCol);
                    }
                }
            }
        }
    }

    /**
     * Check whether colTypes of the <code>selectedCols</code> is string, if not, throw exception.
     *
     * @param tableSchema  TableSchema
     * @param selectedCols the selectedCol to assert.
     */
    public static void assertStringCols(TableSchema tableSchema, String... selectedCols) {
        if (selectedCols != null && selectedCols.length != 0) {
            for (String selectedCol : selectedCols) {
                if (null != selectedCol) {
                    if (!isString(findColType(tableSchema, selectedCol))) {
                        throw new IllegalArgumentException("col type must be string " + selectedCol);
                    }
                }
            }
        }
    }

    /**
     * Check whether colTypes of the <code>selectedCols</code> is vector, if not, throw exception.
     *
     * @param tableSchema  TableSchema
     * @param selectedCols the selectedCol to assert.
     * @see #isVector(TypeInformation)
     */
    public static void assertVectorCols(TableSchema tableSchema, String... selectedCols) {
        if (selectedCols != null && selectedCols.length != 0) {
            for (String selectedCol : selectedCols) {
                if (null != selectedCol) {
                    if (!isVector(findColType(tableSchema, selectedCol))) {
                        throw new IllegalArgumentException("col type must be string " + selectedCol);
                    }
                }
            }
        }
    }

    /**
     * Return the columns in the table whose types are string.
     *
     * @param tableSchema TableSchema
     * @return String columns.
     */
    public static String[] getStringCols(TableSchema tableSchema) {
        return getStringCols(tableSchema, null);
    }

    /**
     * Return the columns in the table whose types are string and are not included in the excludeCols.
     * <p>
     * <p>If <code>excludeCols</code> is null, return all the string columns.
     *
     * @param tableSchema TableSchema.
     * @param excludeCols The columns who are not considered.
     * @return string columns.
     */
    public static String[] getStringCols(TableSchema tableSchema, String[] excludeCols) {
        ArrayList<String> numericCols = new ArrayList<>();
        List<String> excludeColsList = null == excludeCols ? null : Arrays.asList(excludeCols);
        String[] inColNames = tableSchema.getFieldNames();
        TypeInformation<?>[] inColTypes = tableSchema.getFieldTypes();

        for (int i = 0; i < inColNames.length; i++) {
            if (isString(inColTypes[i])) {
                if (null == excludeColsList || !excludeColsList.contains(inColNames[i])) {
                    numericCols.add(inColNames[i]);
                }
            }
        }

        return numericCols.toArray(new String[0]);
    }

    /**
     * Return the columns in the table whose types are numeric.
     *
     * @param tableSchema TableSchema
     * @return numeric columns.
     */
    public static String[] getNumericCols(TableSchema tableSchema) {
        return getNumericCols(tableSchema, null);
    }

    /**
     * Return the columns in the table whose types are numeric and are not included in the excludeCols.
     * <p>
     * <p>If <code>excludeCols</code> is null, return all the numeric columns.
     *
     * @param tableSchema TableSchema.
     * @param excludeCols the columns who are not considered.
     * @return numeric columns.
     */
    public static String[] getNumericCols(TableSchema tableSchema, String[] excludeCols) {
        ArrayList<String> numericCols = new ArrayList<>();
        List<String> excludeColsList = (null == excludeCols ? null : Arrays.asList(excludeCols));
        String[] inColNames = tableSchema.getFieldNames();
        TypeInformation<?>[] inColTypes = tableSchema.getFieldTypes();

        for (int i = 0; i < inColNames.length; i++) {
            if (isNumber(inColTypes[i])) {
                if (null == excludeColsList || !excludeColsList.contains(inColNames[i])) {
                    numericCols.add(inColNames[i]);
                }
            }
        }

        return numericCols.toArray(new String[0]);
    }

    /**
     * Get the columns from featureCols who are included in the <code>categoricalCols</code>, and the columns whose
     * types are string or boolean.
     * <p>If <code>categoricalCols</code> is null, return all the categorical columns.
     *
     * @param tableSchema     TableSchema.
     * @param featureCols     the columns to chosen from.
     * @param categoricalCols the columns which are included in the final result whatever the types of them are. And it
     *                        must be a subset of featureCols.
     * @return the categoricalCols.
     */
    public static String[] getCategoricalCols(
        TableSchema tableSchema, String[] featureCols, String[] categoricalCols) {
        if (null == featureCols) {
            return categoricalCols;
        }
        List<String> categoricalList = null == categoricalCols ? null : Arrays.asList(categoricalCols);
        List<String> featureList = Arrays.asList(featureCols);
        if (null != categoricalCols && !featureList.containsAll(categoricalList)) {
            throw new IllegalArgumentException("CategoricalCols must be included in featureCols!");
        }

        TypeInformation[] featureColTypes = findColTypes(tableSchema, featureCols);
        List<String> res = new ArrayList<>();
        for (int i = 0; i < featureCols.length; i++) {
            boolean included = null != categoricalList && categoricalList.contains(featureCols[i]);
            if (included || Types.BOOLEAN == featureColTypes[i] || Types.STRING == featureColTypes[i]) {
                res.add(featureCols[i]);
            }
        }

        return res.toArray(new String[0]);
    }

    /**
     * format the column names as header of markdown.
     */
    public static String formatTitle(String[] colNames) {
        StringBuilder sbd = new StringBuilder();
        StringBuilder sbdSplitter = new StringBuilder();

        for (int i = 0; i < colNames.length; ++i) {
            if (i > 0) {
                sbd.append("|");
                sbdSplitter.append("|");
            }

            sbd.append(colNames[i]);

            int t = null == colNames[i] ? 4 : colNames[i].length();
            for (int j = 0; j < t; j++) {
                sbdSplitter.append("-");
            }
        }

        return sbd.toString() + "\r\n" + sbdSplitter.toString();
    }

    /**
     * format the row as body of markdown.
     */
    public static String formatRows(Row row) {
        StringBuilder sbd = new StringBuilder();

        for (int i = 0; i < row.getArity(); ++i) {
            if (i > 0) {
                sbd.append("|");
            }
            Object obj = row.getField(i);
            if (obj instanceof Double || obj instanceof Float) {
                sbd.append(String.format("%.4f", ((Number) obj).doubleValue()));
            } else {
                sbd.append(obj);
            }
        }

        return sbd.toString();
    }

    /**
     * format the column names and rows in table as markdown.
     */
    public static String format(String[] colNames, List<Row> data) {
        StringBuilder sbd = new StringBuilder();
        sbd.append(formatTitle(colNames));

        for (Row row : data) {
            sbd.append("\n").append(formatRows(row));
        }

        return sbd.toString();
    }

    /**
     * which method Convert column names array to SQL clause. For example, columns {a, b, c} will be converted
     * into "`a`,`b`,`c`".
     *
     * @param colNames columns to convert
     * @return converted SQL clause.
     */
    public static String columnsToSqlClause(String[] colNames) {
        return Joiner.on("`,`").appendTo(new StringBuilder("`"), colNames).append("`").toString();
    }

    /** open ends here **/

    /**
     * Todo: add the exception throw
     *
     * @param schema
     * @return
     */
    public static String toSchemaJson(TableSchema schema) {
        TypeInformation<?>[] types = schema.getFieldTypes();
        int n = types.length;
        String[] typeStrs = new String[n];
        for (int i = 0; i < n; i++) {
            typeStrs[i] = FlinkTypeConverter.getTypeString(types[i]);
            if (typeStrs[i] == null) {
                typeStrs[i] = VectorTypes.getTypeName(types[i]);
            }
        }
        return gson.toJson(new String[][]{schema.getFieldNames(), typeStrs}, String[][].class);
    }

    public static TableSchema fromSchemaJson(String schemaJson) {
        try {
            String[][] t = gson.fromJson(schemaJson, String[][].class);
            int n = t[1].length;
            TypeInformation<?>[] types = new TypeInformation<?>[n];
            for (int i = 0; i < n; i++) {
                types[i] = FlinkTypeConverter.getFlinkType(t[1][i]);
                if (types[i] == null) {
                    types[i] = VectorTypes.getTypeInformation(t[1][i]);
                }
            }
            return new TableSchema(t[0], types);
        } catch (Exception e) {
            throw new IllegalArgumentException("Fail to getVector TableSchema from string " + schemaJson);
        }
    }

    public static Table concatTables(Table[] tables, Long sessionId) {
        final int[] numCols = new int[tables.length];
        final List<String> allColNames = new ArrayList<>();
        final List<TypeInformation> allColTypes = new ArrayList<>();
        allColNames.add("table_id");
        allColTypes.add(Types.LONG);
        for (int i = 0; i < tables.length; i++) {
            if (tables[i] == null) {
                numCols[i] = 0;
            } else {
                numCols[i] = tables[i].getSchema().getFieldNames().length;
                String[] prefixedColNames = tables[i].getSchema().getFieldNames().clone();
                for (int j = 0; j < prefixedColNames.length; j++) {
                    prefixedColNames[j] = String.format("t%d_%s", i, prefixedColNames[j]);
                }
                allColNames.addAll(Arrays.asList(prefixedColNames));
                allColTypes.addAll(Arrays.asList(tables[i].getSchema().getFieldTypes()));
            }
        }

        if (allColNames.size() == 1) {
            return null;
        }

        DataSet<Row> allRows = null;
        int startCol = 1;
        final int numAllCols = allColNames.size();
        for (int i = 0; i < tables.length; i++) {
            if (tables[i] == null) {
                continue;
            }
            final int constStartCol = startCol;
            final int iTable = i;
            DataSet<Row> rows = BatchOperator.fromTable(tables[i]).setMLEnvironmentId(sessionId).getDataSet();
            rows = rows.map(new RichMapFunction<Row, Row>() {
                transient Row reused;

                @Override
                public void open(Configuration parameters) throws Exception {
                    reused = new Row(numAllCols);
                }

                @Override
                public Row map(Row value) throws Exception {
                    for (int i = 0; i < numAllCols; i++) {
                        reused.setField(i, null);
                    }
                    reused.setField(0, (long) iTable);
                    for (int i = 0; i < numCols[iTable]; i++) {
                        reused.setField(constStartCol + i, value.getField(i));
                    }
                    return reused;
                }
            });
            if (allRows == null) {
                allRows = rows;
            } else {
                allRows = allRows.union(rows);
            }
            startCol += numCols[i];
        }
        return DataSetConversionUtil.toTable(sessionId, allRows, allColNames.toArray(new String[0]),
            allColTypes.toArray(new TypeInformation[0]));
    }

    public static Table[] splitTable(Table table) {
        TableSchema schema = table.getSchema();
        final String[] colNames = schema.getFieldNames();
        String idCol = colNames[0];
        if (!idCol.equalsIgnoreCase("table_id")) {
            throw new IllegalArgumentException("The table can't be splited.");
        }

        String lastCol = colNames[colNames.length - 1];
        int maxTableId = Integer.valueOf(lastCol.substring(1, lastCol.indexOf('_')));
        int numTables = maxTableId + 1;

        int[] numColsOfEachTable = new int[numTables];
        for (int i = 1; i < colNames.length; i++) {
            int tableId = Integer.valueOf(colNames[i].substring(1, lastCol.indexOf('_')));
            numColsOfEachTable[tableId]++;
        }

        Table[] splited = new Table[numTables];
        int startCol = 1;
        for (int i = 0; i < numTables; i++) {
            if (numColsOfEachTable[i] == 0) {
                continue;
            }
            String[] selectedCols = Arrays.copyOfRange(colNames, startCol, startCol + numColsOfEachTable[i]);
            BatchOperator sub = BatchOperator.fromTable(table)
                .where(String.format("%s=%d", "table_id", i))
                .select(selectedCols);

            // recover the col names
            String prefix = String.format("t%d_", i);
            StringBuilder sbd = new StringBuilder();
            for (int j = 0; j < selectedCols.length; j++) {
                if (j > 0) {
                    sbd.append(",");
                }
                sbd.append(selectedCols[j].substring(prefix.length()));
            }
            sub = sub.as(sbd.toString());
            splited[i] = sub.getOutputTable();
            startCol += numColsOfEachTable[i];
        }
        return splited;
    }

    public static Row getRow(Row row, int... keepIdxs){
        Row res = null;
        if(null != keepIdxs) {
            res = new Row(keepIdxs.length);
            for (int i = 0; i < keepIdxs.length; i++) {
                res.setField(i, row.getField(keepIdxs[i]));
            }
        }
        return res;
    }
}
