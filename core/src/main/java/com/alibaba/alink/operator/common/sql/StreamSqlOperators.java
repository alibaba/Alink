package com.alibaba.alink.operator.common.sql;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.stream.StreamOperator;

import org.apache.flink.table.api.java.StreamTableEnvironment;
import scala.collection.immutable.Stream;

/**
 * Apply sql operators(select, where, filter, union etc.) on {@link StreamOperator}s.
 * <p>
 * It is package private to allow access from {@link StreamOperator}.
 */
public class StreamSqlOperators {

    /**
     * Get the {@link MLEnvironment} of the <code>AlgoOperator</code>.
     */
    private static MLEnvironment getMLEnv(AlgoOperator algoOp) {
        return MLEnvironmentFactory.get(algoOp.getMLEnvironmentId());
    }

    /**
     * Register the output table of a StreamOperator to the {@link StreamTableEnvironment}
     * with a temporary table name.
     *
     * @param streamOp The StreamOperator who's output table is being registered.
     * @return The temporary table name.
     */
    private static String registerTempTable(StreamOperator streamOp) {
        StreamTableEnvironment tEnv = getMLEnv(streamOp).getStreamTableEnvironment();
        String tmpTableName = TableUtil.getTempTableName();
        tEnv.registerTable(tmpTableName, streamOp.getOutputTable());
        return tmpTableName;
    }

    /**
     * Evaluate the "select" query on the StreamOperator.
     *
     * @param fields The query fields.
     * @return The evaluation result as a StreamOperator.
     */
    public static StreamOperator select(StreamOperator streamOp, String fields) {
        String tmpTableName = registerTempTable(streamOp);
        return (StreamOperator)(getMLEnv(streamOp)
            .streamSQL(String.format("SELECT %s FROM %s", fields, tmpTableName))
            .setMLEnvironmentId(streamOp.getMLEnvironmentId()));
    }

    /**
     * Rename the fields of a StreamOperator.
     *
     * @param fields Comma separated field names.
     * @return The StreamOperator after renamed.
     */
    public static StreamOperator as(StreamOperator streamOp, String fields) {
        return StreamOperator.fromTable(streamOp.getOutputTable().as(fields))
            .setMLEnvironmentId(streamOp.getMLEnvironmentId());
    }

    /**
     * Apply the "where" operation on the StreamOperator.
     *
     * @param predicate The filter conditions.
     * @return The filter result.
     */
    public static StreamOperator where(StreamOperator streamOp, String predicate) {
        String tmpTableName = registerTempTable(streamOp);
        return (StreamOperator)(getMLEnv(streamOp)
            .streamSQL(String.format("SELECT * FROM %s WHERE %s", tmpTableName, predicate))
            .setMLEnvironmentId(streamOp.getMLEnvironmentId()));
    }

    /**
     * Apply the "filter" operation on the StreamOperator.
     *
     * @param predicate The filter conditions.
     * @return The filter result.
     */
    public static StreamOperator filter(StreamOperator streamOp, String predicate) {
        return where(streamOp, predicate);
    }

    /**
     * Union with another <code>StreamOperator</code>, the duplicated records are kept.
     *
     * @param leftOp  BatchOperator on the left hand side.
     * @param rightOp BatchOperator on the right hand side.
     * @return The resulted <code>StreamOperator</code> of the "unionAll" operation.
     */
    public static StreamOperator unionAll(StreamOperator leftOp, StreamOperator rightOp) {
        return StreamOperator.fromTable(leftOp.getOutputTable().unionAll(rightOp.getOutputTable()))
            .setMLEnvironmentId(leftOp.getMLEnvironmentId());
    }
}
