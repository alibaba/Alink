package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;
import com.alibaba.alink.params.recommendation.BaseSimilarItemsRecommParams;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;

import java.util.List;

public class UserCfRecommKernel extends RecommKernel implements Cloneable {

	private static final long serialVersionUID = 3693021585823090111L;
	private transient ThreadLocal <ItemCfRecommData> model;

	private Integer topN;
	private boolean excludeKnown = false;
	private transient ThreadLocal <double[]> scores;

	public UserCfRecommKernel(TableSchema modelSchema, TableSchema dataSchema, Params params, RecommType recommType) {
		super(modelSchema, dataSchema, params, recommType);
		switch (recommType) {
			case SIMILAR_USERS: {
				this.topN = this.params.get(BaseSimilarItemsRecommParams.K);
				break;
			}
			case USERS_PER_ITEM: {
				this.topN = this.params.get(BaseUsersPerItemRecommParams.K);
				this.excludeKnown = this.params.get(BaseUsersPerItemRecommParams.EXCLUDE_KNOWN);
				break;
			}
			case ITEMS_PER_USER: {
				this.topN = this.params.get(BaseItemsPerUserRecommParams.K);
				break;
			}
			case RATE: {
				break;
			}
			default: {
				throw new AkUnsupportedOperationException("UserCf not support " + recommType + " yet!");
			}
		}
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		for (Row row : modelRows) {
			if (row.getField(0) == null && row.getField(1) == null) {
				Params params = Params.fromJson((String) row.getField(2));
				userColName = params.getString("itemCol");
				itemColName = params.getString("userCol");
			}
		}
		if (recommType.equals(RecommType.USERS_PER_ITEM)) {
			model = ThreadLocal.withInitial(()
				-> new ItemCfRecommModelDataConverter(RecommType.ITEMS_PER_USER).load(modelRows));
		} else if (recommType.equals(RecommType.ITEMS_PER_USER)) {
			model = ThreadLocal.withInitial(()
				-> new ItemCfRecommModelDataConverter(RecommType.USERS_PER_ITEM).load(modelRows));
		} else {
			model = ThreadLocal.withInitial(()
				-> new ItemCfRecommModelDataConverter(recommType).load(modelRows));
		}
		scores = ThreadLocal.withInitial(() -> new double[model.get().items.length]);
		switch (recommType) {
			case ITEMS_PER_USER:
			case SIMILAR_ITEMS: {
				recommObjType = FlinkTypeConverter.getFlinkType(
					model.get().meta.get(ItemCfRecommModelDataConverter.USER_TYPE));
				break;
			}
			case SIMILAR_USERS:
			case USERS_PER_ITEM: {
				recommObjType = FlinkTypeConverter.getFlinkType(
					model.get().meta.get(ItemCfRecommModelDataConverter.ITEM_TYPE));
			}
		}

	}

	@Override
	public Double rate(Object[] ids) {
		Object userId = ids[0];
		Object itemId = ids[1];
		return ItemCfRecommKernel.rate(itemId, userId, model.get());
	}

	@Override
	public MTable recommendItemsPerUser(Object userId) {
		return ItemCfRecommKernel.recommendUsers(userId, model.get(), topN, excludeKnown, itemColName, recommObjType);
	}

	@Override
	public MTable recommendUsersPerItem(Object itemId) {
		return ItemCfRecommKernel.recommendItems(itemId, model.get(), topN, excludeKnown, scores.get(), userColName,
			recommObjType);
	}

	@Override
	public MTable recommendSimilarItems(Object itemId) {
		throw new AkUnsupportedOperationException("ItemCf not support recommendSimilarItems");
	}

	@Override
	public MTable recommendSimilarUsers(Object userId) {
		return ItemCfRecommKernel.findSimilarItems(userId, model.get(), topN, userColName, recommObjType);
	}

	@Override
	public RecommKernel createNew() {
		return new UserCfRecommKernel(getModelSchema(), getDataSchema(), params.clone(), recommType);
	}
}
