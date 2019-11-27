package com.alibaba.alink.operator.common.dataproc;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Map;

/**
 * The model data of {@link com.alibaba.alink.pipeline.dataproc.MultiStringIndexerModel}.
 */
public class MultiStringIndexerModelData {
    /**
     * The meta data.
     */
    public Params meta;

    /**
     * The mapping from token to index. A list of (column index, token, token index)
     */
    List<Tuple3<Integer, String, Long>> tokenAndIndex;

    /**
     * Number of tokens of each columns.
     */
    Map<Integer, Long> tokenNumber;

    /**
     * Get the number of tokens of a column.
     *
     * @param columnName Name of the column.
     * @return The number of tokens of that column.
     */
    public long getNumberOfTokensOfColumn(String columnName) {
        int colIndex = TableUtil.findColIndex(meta.get(HasSelectedCols.SELECTED_COLS), columnName);
        Preconditions.checkArgument(colIndex >= 0, "Can't find column: " + columnName);
        Preconditions.checkArgument(tokenNumber != null);
        return tokenNumber.get(colIndex);
    }
}