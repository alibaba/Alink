package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;
import com.alibaba.alink.params.recommendation.BaseRateRecommParams;
import com.alibaba.alink.params.recommendation.BaseSimilarItemsRecommParams;
import com.alibaba.alink.params.recommendation.BaseSimilarUsersRecommParams;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;

import java.util.List;

public class UserCfRecommKernel extends RecommKernel implements Cloneable {

	private static final long serialVersionUID = 3693021585823090111L;
	private ItemCfRecommData model = null;
	private int userColIdx = -1;
	private int itemColIdx = -1;
	private Integer topN;
	private boolean excludeKnown = false;
	private double[] scores = null;

	public UserCfRecommKernel(TableSchema modelSchema, TableSchema dataSchema, Params params, RecommType recommType) {
		super(modelSchema, dataSchema, params, recommType);
		switch (recommType) {
			case SIMILAR_USERS: {
				String userColName = this.params.get(BaseSimilarUsersRecommParams.USER_COL);
				this.userColIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), userColName);
				this.topN = this.params.get(BaseSimilarItemsRecommParams.K);
				break;
			}
			case USERS_PER_ITEM: {
				String itemColName = this.params.get(BaseUsersPerItemRecommParams.ITEM_COL);
				this.itemColIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), itemColName);
				this.topN = this.params.get(BaseUsersPerItemRecommParams.K);
				this.excludeKnown = this.params.get(BaseUsersPerItemRecommParams.EXCLUDE_KNOWN);
				break;
			}
			case ITEMS_PER_USER: {
				String userColName = this.params.get(BaseItemsPerUserRecommParams.USER_COL);
				this.userColIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), userColName);
				this.topN = this.params.get(BaseItemsPerUserRecommParams.K);
				break;
			}
			case RATE: {
				String itemColName = this.params.get(BaseRateRecommParams.ITEM_COL);
				this.itemColIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), itemColName);
				String userColName = this.params.get(BaseRateRecommParams.USER_COL);
				this.userColIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), userColName);
				break;
			}
			default: {
				throw new RuntimeException("UserCf not support " + recommType + " yet!");
			}
		}
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		if (recommType.equals(RecommType.USERS_PER_ITEM)) {
			this.model = new ItemCfRecommModelDataConverter(RecommType.ITEMS_PER_USER).load(modelRows);
		} else if (recommType.equals(RecommType.ITEMS_PER_USER)) {
			this.model = new ItemCfRecommModelDataConverter(RecommType.USERS_PER_ITEM).load(modelRows);
		} else {
			this.model = new ItemCfRecommModelDataConverter(recommType).load(modelRows);
		}
	}

	@Override
	public Double rate(Row infoUserItem) {
		Object userId = infoUserItem.getField(userColIdx);
		Object itemId = infoUserItem.getField(itemColIdx);
		return ItemCfRecommKernel.rate(itemId, userId, model);
	}

	@Override
	public String recommendItemsPerUser(Row infoUser) {
		Object userId = infoUser.getField(userColIdx);
		return ItemCfRecommKernel.recommendUsers(userId, model, topN, excludeKnown);
	}

	@Override
	public String recommendUsersPerItem(Row infoItem) {
		Object itemId = infoItem.getField(itemColIdx);
		if (null == scores) {
			scores = new double[model.items.length];
		}
		return ItemCfRecommKernel.recommendItems(itemId, model, topN, excludeKnown, scores);
	}

	@Override
	public String recommendSimilarItems(Row infoItem) {
		throw new RuntimeException("ItemCf not support recommendSimilarItems");
	}

	@Override
	public String recommendSimilarUsers(Row infoUser) {
		Object userId = infoUser.getField(userColIdx);
		return ItemCfRecommKernel.findSimilarItems(userId, model, topN);
	}

	@Override
	protected RecommKernel mirror() {
		try {
			UserCfRecommKernel kernel = (UserCfRecommKernel) this.clone();
			return kernel;
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}
}
