package com.alibaba.alink.operator.common.feature;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.feature.OneHotPredictParams;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

/**
 * This mapper maps some table columns to a binary vector. It encoding some columns to a key-value format.
 */
public class OneHotModelMapper extends ModelMapper {
    private double[] values;
    private int[] indices;
    private String[] colNames;
    private OutputColsHelper outputColsHelper;
    private OneHotModelData modelData;

    /**
     * Constructor.
     *
     * @param modelSchema the model schema.
     * @param dataSchema  the data schema.
     * @param params      the parameters of mapper.
     */
    public OneHotModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);
        this.colNames = dataSchema.getFieldNames();
        String[] reservedColNames = params.get(OneHotPredictParams.RESERVED_COLS);
        String predColName = this.params.get(OneHotPredictParams.OUTPUT_COL);
        this.outputColsHelper = new OutputColsHelper(dataSchema, predColName, VectorTypes.VECTOR, reservedColNames);
    }

    /**
     * Load row type model and save it in OneHotModelData.
     *
     * @param modelRows row type model.
     */
    @Override
    public void loadModel(List<Row> modelRows) {
        modelData = new OneHotModelDataConverter().load(modelRows);
        this.values = new double[modelData.mapping.size()];
        this.indices = new int[modelData.mapping.size()];
    }

    /**
     * Get output schema.
     *
     * @return output schema.
     */
    @Override
    public TableSchema getOutputSchema() {
        return outputColsHelper.getResultSchema();
    }

    /**
     * map process.
     *
     * @param src input row.
     * @return kv format result.
     */
    @Override
    public Row map(Row src) throws Exception {

        int iter = 0;
        for (Map.Entry<String, HashMap<String, Integer>> entry : this.modelData.mapping.entrySet()) {
            String colName = entry.getKey();
            HashMap<String, Integer> innerMap = entry.getValue();
            int idx = TableUtil.findColIndex(this.colNames, colName);
            String val = (src.getField(idx) != null) ? src.getField(idx).toString() : "null";
            Integer key = innerMap.get(val);
            if (null != key) {
                indices[iter++] = key;
            } else {
                indices[iter++] = this.modelData.otherMapping;
            }
        }
        Arrays.fill(this.values, 1.0);
        SparseVector vec = new SparseVector(modelData.meta.get(ModelParamName.VECTOR_SIZE), indices, values);
        return this.outputColsHelper.getResultRow(src, Row.of(vec));
    }

}
