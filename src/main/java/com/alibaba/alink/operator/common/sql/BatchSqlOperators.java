package com.alibaba.alink.operator.common.sql;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Apply sql operators(select, where, groupby, join, etc.) on {@link BatchOperator}s.
 * <p>
 * It is package private to allow access from {@link BatchOperator}.
 */
public class BatchSqlOperators {

    /**
     * Get the {@link MLEnvironment} of the <code>AlgoOperator</code>.
     */
    private static MLEnvironment getMLEnv(AlgoOperator algoOp) {
        return MLEnvironmentFactory.get(algoOp.getMLEnvironmentId());
    }

    /**
     * Register the output table of a BatchOperator to the {@link BatchTableEnvironment}
     * with a temporary table name.
     *
     * @param batchOp The BatchOperator who's output table is being registered.
     * @return The temporary table name.
     */
    private static String registerTempTable(BatchOperator batchOp) {
        BatchTableEnvironment tEnv = getMLEnv(batchOp).getBatchTableEnvironment();
        String tmpTableName = TableUtil.getTempTableName();
        tEnv.registerTable(tmpTableName, batchOp.getOutputTable());
        return tmpTableName;
    }

    /**
     * Evaluate the "select" query on the BatchOperator.
     *
     * @param fields The query fields.
     * @return The evaluation result as a BatchOperator.
     */
    public static BatchOperator select(BatchOperator batchOp, String fields) {
        String tmpTableName = registerTempTable(batchOp);
        return getMLEnv(batchOp).batchSQL(String.format("SELECT %s FROM %s", fields, tmpTableName))
            .setMLEnvironmentId(batchOp.getMLEnvironmentId());
    }

    /**
     * Rename the fields of a BatchOperator.
     *
     * @param fields Comma separated field names.
     * @return The BatchOperator after renamed.
     */
    public static BatchOperator as(BatchOperator batchOp, String fields) {
        return BatchOperator.fromTable(batchOp.getOutputTable().as(fields))
            .setMLEnvironmentId(batchOp.getMLEnvironmentId());
    }

    /**
     * Apply the "where" operation on the BatchOperator.
     *
     * @param predicate The filter conditions.
     * @return The filter result.
     */
    public static BatchOperator where(BatchOperator batchOp, String predicate) {
        String tmpTableName = registerTempTable(batchOp);
        return getMLEnv(batchOp).batchSQL(String.format("SELECT * FROM %s WHERE %s", tmpTableName, predicate))
            .setMLEnvironmentId(batchOp.getMLEnvironmentId());
    }

    /**
     * Apply the "filter" operation on the BatchOperator.
     *
     * @param predicate The filter conditions.
     * @return The filter result.
     */
    public static BatchOperator filter(BatchOperator batchOp, String predicate) {
        return where(batchOp, predicate);
    }

    /**
     * Remove duplicated records.
     *
     * @return The resulted <code>BatchOperator</code> of the "distinct" operation.
     */
    public static BatchOperator distinct(BatchOperator batchOp) {
        String tmpTableName = registerTempTable(batchOp);
        return getMLEnv(batchOp).batchSQL(String.format("SELECT DISTINCT * FROM %s", tmpTableName))
            .setMLEnvironmentId(batchOp.getMLEnvironmentId());
    }

    /**
     * Order the records by a specific field.
     *
     * @param fieldName The name of the field by which the records are ordered.
     * @return The resulted <code>BatchOperator</code> of the "orderBy" operation.
     */
    public static BatchOperator orderBy(BatchOperator batchOp, String fieldName, boolean isAscending) {
        return orderByImpl(batchOp, fieldName, isAscending, -1, -1, -1);
    }

    /**
     * Order the records by a specific field and keeping a limited number of records.
     *
     * @param fieldName The name of the field by which the records are ordered.
     * @param limit     The maximum number of records to keep.
     * @return The resulted <code>BatchOperator</code> of the "orderBy" operation.
     */
    public static BatchOperator orderBy(BatchOperator batchOp, String fieldName, boolean isAscending, int limit) {
        return orderByImpl(batchOp, fieldName, isAscending, limit, -1, -1);
    }

    /**
     * Order the records by a specific field and keeping a specific range of records.
     *
     * @param fieldName The name of the field by which the records are ordered.
     * @param offset    The starting position of records to keep.
     * @param fetch     The  number of records to keep.
     * @return The resulted <code>BatchOperator</code> of the "orderBy" operation.
     */
    public static BatchOperator orderBy(BatchOperator batchOp, String fieldName, boolean isAscending, int offset, int fetch) {
        return orderByImpl(batchOp, fieldName, isAscending, -1, offset, fetch);
    }

    /**
     * Order the records by a specific field and keeping a specific range of records.
     *
     * @param fieldName The name of the field by which the records are ordered.
     * @param offset    The starting position of records to keep.
     * @param fetch     The  number of records to keep.
     * @return The resulted <code>BatchOperator</code> of the "orderBy" operation.
     */
    private static BatchOperator orderByImpl(BatchOperator batchOp, String fieldName, boolean isAscending, int limit, int offset, int fetch) {
        String tmpTableName = registerTempTable(batchOp);
        StringBuilder s = new StringBuilder();
        s.append("SELECT * FROM ").append(tmpTableName).append(" ORDER BY ").append(fieldName).append(" ")
            .append(isAscending ? "ASC" : "DESC");
        if (limit >= 0) {
            s.append(" LIMIT ").append(limit);
        }
        if (offset >= 0) {
            s.append(" OFFSET ").append(offset).append( " ROW ");
        }
        if (fetch >= 0) {
            s.append(" FETCH FIRST ").append(fetch).append(" ROW ONLY");
        }
        return getMLEnv(batchOp).batchSQL(s.toString()).setMLEnvironmentId(batchOp.getMLEnvironmentId());
    }

    /**
     * Apply the "group by" operation.
     *
     * @param groupByPredicate The fields by which records are grouped.
     * @param fields           The fields to select after group by.
     * @return The resulted <code>BatchOperator</code> of the "groupBy" operation.
     */
    public static BatchOperator groupBy(BatchOperator batchOp, String groupByPredicate, String fields) {
        String tmpTableName = registerTempTable(batchOp);
        return getMLEnv(batchOp).batchSQL(String.format("SELECT %s FROM %s GROUP BY %s",
            fields, tmpTableName, groupByPredicate))
            .setMLEnvironmentId(batchOp.getMLEnvironmentId());
    }

    /**
     * Implementation of JOIN, LEFT OUTER JOIN, RIGHT OUTER JOIN, and FULL OUTER JOIN.
     */
    private static BatchOperator joinImpl(BatchOperator leftOp, BatchOperator rightOp,
                                          String joinPredicate, String selectClause,
                                          String joinType) {
        String tmpTableName1 = registerTempTable(leftOp);
        String tmpTableName2 = registerTempTable(rightOp);

        return getMLEnv(leftOp).batchSQL(String.format("SELECT %s FROM %s AS a %s %s AS b ON %s",
            selectClause, tmpTableName1, joinType, tmpTableName2, joinPredicate))
            .setMLEnvironmentId(leftOp.getMLEnvironmentId());
    }

    /**
     * Join with another <code>BatchOperator</code>.
     *
     * @param leftOp        BatchOperator on the left hand side.
     * @param rightOp       BatchOperator on the right hand side.
     * @param joinPredicate The predicate specifying the join conditions.
     * @param fields        The clause specifying the fields to select.
     * @return The resulted <code>BatchOperator</code> of the "join" operation.
     */
    public static BatchOperator join(BatchOperator leftOp, BatchOperator rightOp, String joinPredicate, String fields) {
        return joinImpl(leftOp, rightOp, joinPredicate, fields, "JOIN");
    }

    /**
     * Left outer join with another <code>BatchOperator</code>.
     *
     * @param leftOp        BatchOperator on the left hand side.
     * @param rightOp       BatchOperator on the right hand side.
     * @param joinPredicate The predicate specifying the join conditions.
     * @param fields        The clause specifying the fields to select.
     * @return The resulted <code>BatchOperator</code> of the "left outer join" operation.
     */
    public static BatchOperator leftOuterJoin(BatchOperator leftOp, BatchOperator rightOp, String joinPredicate, String fields) {
        return joinImpl(leftOp, rightOp, joinPredicate, fields, "LEFT OUTER JOIN");
    }

    /**
     * Right outer join with another <code>BatchOperator</code>.
     *
     * @param leftOp        BatchOperator on the left hand side.
     * @param rightOp       BatchOperator on the right hand side.
     * @param joinPredicate The predicate specifying the join conditions.
     * @param fields        The clause specifying the fields to select.
     * @return The resulted <code>BatchOperator</code> of the "right outer join" operation.
     */
    public static BatchOperator rightOuterJoin(BatchOperator leftOp, BatchOperator rightOp, String joinPredicate, String fields) {
        return joinImpl(leftOp, rightOp, joinPredicate, fields, "RIGHT OUTER JOIN");
    }

    /**
     * Full outer join with another <code>BatchOperator</code>.
     *
     * @param leftOp        BatchOperator on the left hand side.
     * @param rightOp       BatchOperator on the right hand side.
     * @param joinPredicate The predicate specifying the join conditions.
     * @param fields        The clause specifying the fields to select.
     * @return The resulted <code>BatchOperator</code> of the "full outer join" operation.
     */
    public static BatchOperator fullOuterJoin(BatchOperator leftOp, BatchOperator rightOp, String joinPredicate, String fields) {
        return joinImpl(leftOp, rightOp, joinPredicate, fields, "FULL OUTER JOIN");
    }

    /**
     * Union with another <code>BatchOperator</code>, the duplicated records are removed.
     *
     * @param leftOp  BatchOperator on the left hand side.
     * @param rightOp BatchOperator on the right hand side.
     * @return The resulted <code>BatchOperator</code> of the "union" operation.
     */
    public static BatchOperator union(BatchOperator leftOp, BatchOperator rightOp) {
        return BatchOperator.fromTable(leftOp.getOutputTable().union(rightOp.getOutput()))
            .setMLEnvironmentId(leftOp.getMLEnvironmentId());
    }

    /**
     * Union with another <code>BatchOperator</code>, the duplicated records are kept.
     *
     * @param leftOp  BatchOperator on the left hand side.
     * @param rightOp BatchOperator on the right hand side.
     * @return The resulted <code>BatchOperator</code> of the "unionAll" operation.
     */
    public static BatchOperator unionAll(BatchOperator leftOp, BatchOperator rightOp) {
        return BatchOperator.fromTable(leftOp.getOutputTable().unionAll(rightOp.getOutput()))
            .setMLEnvironmentId(leftOp.getMLEnvironmentId());
    }

    /**
     * Intersect with another <code>BatchOperator</code>, the duplicated records are removed.
     *
     * @param leftOp  BatchOperator on the left hand side.
     * @param rightOp BatchOperator on the right hand side.
     * @return The resulted <code>BatchOperator</code> of the "intersect" operation.
     */
    public static BatchOperator intersect(BatchOperator leftOp, BatchOperator rightOp) {
        return BatchOperator.fromTable(leftOp.getOutputTable().intersect(rightOp.getOutputTable()))
            .setMLEnvironmentId(leftOp.getMLEnvironmentId());
    }

    /**
     * Intersect with another <code>BatchOperator</code>, the duplicated records are kept.
     *
     * @param leftOp  BatchOperator on the left hand side.
     * @param rightOp BatchOperator on the right hand side.
     * @return The resulted <code>BatchOperator</code> of the "intersectAll" operation.
     */
    public static BatchOperator intersectAll(BatchOperator leftOp, BatchOperator rightOp) {
        return BatchOperator.fromTable(leftOp.getOutputTable().intersectAll(rightOp.getOutputTable()))
            .setMLEnvironmentId(leftOp.getMLEnvironmentId());
    }

    /**
     * Minus with another <code>BatchOperator</code>, the duplicated records are removed.
     *
     * @param leftOp  BatchOperator on the left hand side.
     * @param rightOp BatchOperator on the right hand side.
     * @return The resulted <code>BatchOperator</code> of the "minus" operation.
     */
    public static BatchOperator minus(BatchOperator leftOp, BatchOperator rightOp) {
        return BatchOperator.fromTable(leftOp.getOutputTable().minus(rightOp.getOutputTable()))
            .setMLEnvironmentId(leftOp.getMLEnvironmentId());
    }

    /**
     * Minus with another <code>BatchOperator</code>, the duplicated records are kept.
     *
     * @param leftOp  BatchOperator on the left hand side.
     * @param rightOp BatchOperator on the right hand side.
     * @return The resulted <code>BatchOperator</code> of the "minusAll" operation.
     */
    public static BatchOperator minusAll(BatchOperator leftOp, BatchOperator rightOp) {
        return BatchOperator.fromTable(leftOp.getOutputTable().minusAll(rightOp.getOutputTable()))
            .setMLEnvironmentId(leftOp.getMLEnvironmentId());
    }

    private enum CrossType {
        Auto,
        WithTiny,
        WithHuge
    }

    /**
     * Implementation of cross.
     */
    private static BatchOperator crossImpl(BatchOperator leftOp, BatchOperator rightOp,
                                           CrossType type) {
        DataSet<Row> ds1 = leftOp.getDataSet();
        DataSet<Row> ds2 = rightOp.getDataSet();
        DataSet<Row> output = null;
        switch (type) {
            case WithHuge:
                output = ds1.crossWithHuge(ds2).with(new CrossFunc());
                break;
            case WithTiny:
                output = ds1.crossWithTiny(ds2).with(new CrossFunc());
                break;
            default:
                output = ds1.cross(ds2).with(new CrossFunc());
        }
        String[] fieldNames = ArrayUtils.addAll(leftOp.getColNames(), rightOp.getColNames());
        TypeInformation[] fieldTypes = ArrayUtils.addAll(leftOp.getColTypes(), rightOp.getColTypes());
        MLEnvironment mlEnv = getMLEnv(leftOp);
        return BatchOperator.fromTable(DataSetConversionUtil.toTable(mlEnv, output, fieldNames, fieldTypes))
            .setMLEnvironmentId(leftOp.getMLEnvironmentId());
    }

    private static class CrossFunc implements CrossFunction<Row, Row, Row> {
        @Override
        public Row cross(Row in1, Row in2) throws Exception {
            int n1 = in1.getArity();
            int n2 = in2.getArity();
            Row r = new Row(n1 + n2);
            for (int i = 0; i < n1; i++) {
                r.setField(i, in1.getField(i));
            }
            for (int i = 0; i < n2; i++) {
                r.setField(i + n1, in2.getField(i));
            }
            return r;
        }
    }

    /**
     * Cross with another BatchOperator.
     *
     * @param leftOp  BatchOperator on the left hand side.
     * @param rightOp BatchOperator on the right hand side.
     * @return The cross result.
     */
    public static BatchOperator cross(BatchOperator leftOp, BatchOperator rightOp) {
        return crossImpl(leftOp, rightOp, CrossType.Auto);
    }

    /**
     * Cross with another BatchOperator whose size is tiny.
     *
     * @param leftOp  BatchOperator on the left hand side.
     * @param rightOp BatchOperator on the right hand side.
     * @return The cross result.
     */
    public static BatchOperator crossWithTiny(BatchOperator leftOp, BatchOperator rightOp) {
        return crossImpl(leftOp, rightOp, CrossType.WithTiny);

    }

    /**
     * Cross with another BatchOperator whose size is huge.
     *
     * @param leftOp  BatchOperator on the left hand side.
     * @param rightOp BatchOperator on the right hand side.
     * @return The cross result.
     */
    public static BatchOperator crossWithHuge(BatchOperator leftOp, BatchOperator rightOp) {
        return crossImpl(leftOp, rightOp, CrossType.WithHuge);
    }
}
