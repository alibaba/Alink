package com.alibaba.alink.operator.common.utils;

import com.alibaba.alink.common.utils.TableUtil;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class UDFHelper {

    public static String generateRandomFuncName() {
        return "func_" + UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * Generate necessary sql clauses to call Table API to process UDF
     *
     * @param inTableName  the registered input table name
     * @param functionName the registered function name
     * @param outputCol    output column
     * @param selectedCols selected columns
     * @param reservedCols reserved columns
     * @return the sql clause
     */
    public static String generateUDFClause(
        final String inTableName, final String functionName,
        final String outputCol, final String[] selectedCols, final String[] reservedCols) {
        final String selectedColsStr = TableUtil.columnsToSqlClause(selectedCols);

        // if outputCol is found in reservedCols, the one in reservedCols will not be output
        final String[] cleanedReservedCols = Arrays.stream(reservedCols)
            .filter(d -> !d.equals(outputCol))
            .toArray(String[]::new);

        StringBuilder sb = new StringBuilder();
        if (cleanedReservedCols.length > 0) {
            sb.append(TableUtil.columnsToSqlClause(cleanedReservedCols)).append(", ");
        }
        sb.append(functionName).append("(").append(selectedColsStr).append(") as `").append(outputCol).append("`");
        return String.format("SELECT %s FROM %s", sb.toString(), inTableName);
    }

    /**
     * Generate sql clause to call sqlQuery to process UDTF
     *
     * @param inTableName  the registered input table name
     * @param functionName the registered function name
     * @param outputCols   output columns
     * @param selectedCols selected columns
     * @param reservedCols reserved columns
     * @param joinType     cross or leftOuter
     * @return the sql clause
     */
    public static String generateUDTFClause(
        final String inTableName, final String functionName,
        final String[] outputCols, final String[] selectedCols, final String[] reservedCols, final String joinType) {

        final String selectedColsStr = TableUtil.columnsToSqlClause(selectedCols);

        // always shade outputCols to avoid potential conflicts with selectedCols
        final String[] shadedOutputCols = Arrays.stream(outputCols)
            .map(d -> d + "_" + UUID.randomUUID().toString().replace("-", ""))
            .toArray(String[]::new);
        final String shadedOutputColsStr = TableUtil.columnsToSqlClause(shadedOutputCols);

        // if one of outputCols is found in reservedCols, the one in reservedCols will not be output
        final Set<String> outputColsSet = new HashSet<>(Arrays.asList(outputCols));
        final String[] cleanedReservedCols = Arrays.stream(reservedCols)
            .filter(d -> !outputColsSet.contains(d))
            .toArray(String[]::new);

        StringBuilder sb = new StringBuilder();
        if (cleanedReservedCols.length > 0) {
            sb.append(TableUtil.columnsToSqlClause(cleanedReservedCols)).append(", ");
        }
        for (int i = 0; i < outputCols.length; i += 1) {
            sb.append("`").append(shadedOutputCols[i]).append("` as `").append(outputCols[i]).append("`");
            if (i < outputCols.length - 1) {
                sb.append(", ");
            }
        }
        String selectClause = sb.toString();

        // generate the join clause
        final String crossJoinTemplate = "SELECT %s FROM %s, LATERAL TABLE(%s(%s)) as T(%s)";
        final String leftJoinTemplate = "SELECT %s FROM %s LEFT JOIN LATERAL TABLE(%s(%s)) as T(%s) ON TRUE";

        final String joinTemplate = joinType.equalsIgnoreCase("CROSS")
            ? crossJoinTemplate
            : leftJoinTemplate;

        final String[] finalOutputCols = ArrayUtils.addAll(cleanedReservedCols, shadedOutputCols);
        final String finalOutputColsStr = TableUtil.columnsToSqlClause(finalOutputCols);

        final String joinClause = String.format(joinTemplate,
            finalOutputColsStr, inTableName, functionName, selectedColsStr, shadedOutputColsStr);

        return String.format("SELECT %s FROM (%s)", selectClause, joinClause);
    }

}
