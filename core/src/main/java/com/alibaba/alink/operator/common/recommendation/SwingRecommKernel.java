package com.alibaba.alink.operator.common.recommendation;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.recommendation.BaseSimilarItemsRecommParams;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;

public class SwingRecommKernel extends RecommKernel implements Cloneable {
	private HashMap <Comparable <?>, SwingResData> itemRecomm;
	private final Integer topN;

	public SwingRecommKernel(TableSchema modelSchema, TableSchema dataSchema, Params params, RecommType recommType) {
		super(modelSchema, dataSchema, params, recommType);
		if (recommType == RecommType.SIMILAR_ITEMS) {
			this.topN = this.params.get(BaseSimilarItemsRecommParams.K);
			this.recommObjType = modelSchema.getFieldTypes()[0];
		} else {
			throw new RuntimeException("ItemKnn not support " + recommType + " yet!");
		}
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		int itemNumber = modelRows.size();
		itemRecomm = new HashMap <>(itemNumber);
		for (Row modelRow : modelRows) {
			String jsonModel = (String) modelRow.getField(1);
			itemRecomm.put((Comparable <?>) modelRow.getField(0),
				JsonConverter.fromJson(jsonModel, SwingResData.class));
		}
	}

	@Override
	public Double rate(Object[] infoUserItem) {
		throw new RuntimeException("Swing not support rate.");
	}

	@Override
	public MTable recommendItemsPerUser(Object userId) {
		throw new RuntimeException("Swing not support recommendItemsPerUser.");
	}

	@Override
	public MTable recommendUsersPerItem(Object itemId) {
		throw new RuntimeException("Swing not support recommendItemsPerUser.");
	}

	@Override
	public MTable recommendSimilarItems(Object itemId) {
		Comparable <?> mainItem = (Comparable <?>) itemId;
		if (!itemRecomm.containsKey(mainItem)) {
			return null;
		}
		SwingResData items = itemRecomm.get(itemId);
		return items.returnTopNData(topN, recommObjType);
	}

	@Override
	public MTable recommendSimilarUsers(Object userId) {
		throw new RuntimeException("Swing not support recommendItemsPerUser.");
	}
}