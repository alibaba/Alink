package com.alibaba.alink.operator.common.recommendation;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.recommendation.AlsPredictParams;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * The ModelMapper for {@link com.alibaba.alink.pipeline.recommendation.ALSModel}.
 */
public class AlsModelMapper extends ModelMapper {

    private AlsModelData model = null;
    private int userColIdx = -1;
    private int itemColIdx = -1;
    private OutputColsHelper outputColsHelper;

    public AlsModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);
        String predResultColName = this.params.get(AlsPredictParams.PREDICTION_COL);
        String userColName = this.params.get(AlsPredictParams.USER_COL);
        this.userColIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), userColName);
        String itemColName = this.params.get(AlsPredictParams.ITEM_COL);
        this.itemColIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), itemColName);
        this.outputColsHelper = new OutputColsHelper(dataSchema, predResultColName, Types.DOUBLE());
    }

    @Override
    public void loadModel(List<Row> modelRows) {
        String userCol = super.getModelSchema().getFieldNames()[0];
        String itemCol = super.getModelSchema().getFieldNames()[1];
        this.model = new AlsModelDataConverter(userCol, itemCol).load(modelRows);
    }

    @Override
    public TableSchema getOutputSchema() {
        return outputColsHelper.getResultSchema();
    }

    @Override
    public Row map(Row row) throws Exception {
        return outputColsHelper.getResultRow(row, Row.of(predictRating(row, userColIdx, itemColIdx)));
    }

    private Double predictRating(Row row, int userColIdx, int itemColIdx) throws Exception {
        long userId = ((Number) row.getField(userColIdx)).longValue();
        long itemId = ((Number) row.getField(itemColIdx)).longValue();

        int numUsers = model.userFactors.length;
        int numItems = model.itemFactors.length;

        int userLocalId = model.userIdMap.getOrDefault(userId, numUsers);
        int itemLocalId = model.itemIdMap.getOrDefault(itemId, numItems);

        if (userLocalId < numUsers && itemLocalId < numItems) {
            float[] predUserFactors = model.userFactors[userLocalId];
            float[] predItemFactors = model.itemFactors[itemLocalId];
            double v = 0.;
            int n = predUserFactors.length;
            for (int i = 0; i < n; i++) {
                v += predUserFactors[i] * predItemFactors[i];
            }
            return v;
        } else {
            return null;
        }
    }
}
