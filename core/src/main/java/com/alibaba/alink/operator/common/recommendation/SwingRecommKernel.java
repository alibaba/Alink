package com.alibaba.alink.operator.common.recommendation;


import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.recommendation.BaseSimilarItemsRecommParams;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SwingRecommKernel extends RecommKernel implements Cloneable {
    private HashMap<Comparable, SwingResData> itemRecomm;
    private Integer topN;

    public SwingRecommKernel(TableSchema modelSchema, TableSchema dataSchema, Params params, RecommType recommType) {
        super(modelSchema, dataSchema, params, recommType);
        switch (recommType) {
            case SIMILAR_ITEMS: {
                this.topN = this.params.get(BaseSimilarItemsRecommParams.K);
                break;
            }
            default: {
                throw new RuntimeException("ItemKnn not support " + recommType + " yet!");
            }
        }
    }

    @Override
    public void loadModel(List<Row> modelRows) {
        int itemNumber = modelRows.size();
        itemRecomm = new HashMap<>(itemNumber);
        for (Row modelRow : modelRows) {
            String jsonModel = (String) modelRow.getField(1);
            itemRecomm.put((Comparable) modelRow.getField(0),
                JsonConverter.fromJson(jsonModel, SwingResData.class));
        }
    }

    @Override
    public Double rate(Object[] infoUserItem) throws Exception {
        throw new RuntimeException("Swing not support rate.");
    }

    @Override
    public String recommendItemsPerUser(Object userId) throws Exception {
        throw new RuntimeException("Swing not support recommendItemsPerUser.");
    }

    @Override
    public String recommendUsersPerItem(Object itemId) throws Exception {
        throw new RuntimeException("Swing not support recommendItemsPerUser.");
    }

    @Override
    public String recommendSimilarItems(Object itemId) throws Exception {
        Comparable mainItem = (Comparable) itemId;
        if (!itemRecomm.containsKey(mainItem)) {
            return null;
        }
        SwingResData items = itemRecomm.get(itemId);
        return items.returnTopNData(topN);
    }


    @Override
    public String recommendSimilarUsers(Object userId) throws Exception {
        throw new RuntimeException("Swing not support recommendItemsPerUser.");
    }
}