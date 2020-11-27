package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.utils.PackBatchOperatorUtil;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;
import com.alibaba.alink.params.recommendation.BaseRateRecommParams;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlsRecommKernel extends RecommKernel {

	private static final long serialVersionUID = 2716744280007281817L;
	protected transient Map <Object, DenseVector> userFactors;
	protected transient Map <Object, DenseVector> itemFactors;
	protected transient Map <Object, Set <Object>> historyUserItems;
	protected transient Map <Object, Set <Object>> historyItemUsers;

	private int userColIdx = -1;
	private int itemColIdx = -1;
	private Integer topK;
	private boolean excludeKnown = false;

	public AlsRecommKernel(TableSchema modelSchema, TableSchema dataSchema, Params params, RecommType recommType) {
		super(modelSchema, dataSchema, params, recommType);
		String userColName = getParamDefaultAsNull(params, BaseRateRecommParams.USER_COL);
		String itemColName = getParamDefaultAsNull(params, BaseRateRecommParams.ITEM_COL);
		this.topK = getParamDefaultAsNull(params, BaseItemsPerUserRecommParams.K);
		if (userColName != null) {
			this.userColIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), userColName);
		}
		if (itemColName != null) {
			this.itemColIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), itemColName);
		}

		if (recommType == RecommType.RATE) {
			Preconditions.checkArgument(userColIdx >= 0, "Can't find user col: " + userColName);
			Preconditions.checkArgument(itemColIdx >= 0, "Can't find item col: " + itemColName);
		} else if (recommType == RecommType.ITEMS_PER_USER) {
			Preconditions.checkArgument(userColIdx >= 0, "Can't find user col: " + userColName);
			Preconditions.checkArgument(topK != null, "Missing param topK");
			excludeKnown = params.get(BaseItemsPerUserRecommParams.EXCLUDE_KNOWN);
		} else if (recommType == RecommType.USERS_PER_ITEM) {
			Preconditions.checkArgument(itemColIdx >= 0, "Can't find item col: " + itemColName);
			Preconditions.checkArgument(topK != null, "Missing param topK");
			excludeKnown = params.get(BaseUsersPerItemRecommParams.EXCLUDE_KNOWN);
		} else if (recommType == RecommType.SIMILAR_USERS) {
			Preconditions.checkArgument(userColIdx >= 0, "Can't find user col: " + userColName);
			Preconditions.checkArgument(topK != null, "Missing param topK");
		} else if (recommType == RecommType.SIMILAR_ITEMS) {
			Preconditions.checkArgument(itemColIdx >= 0, "Can't find item col: " + itemColName);
			Preconditions.checkArgument(topK != null, "Missing param topK");
		} else {
			throw new UnsupportedOperationException("Not supported rec type.");
		}
	}

	private static <T> T getParamDefaultAsNull(Params params, ParamInfo <T> paramInfo) {
		if (params.contains(paramInfo)) {
			return params.get(paramInfo);
		} else {
			if (paramInfo.hasDefaultValue()) {
				return paramInfo.getDefaultValue();
			} else {
				return null;
			}
		}
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		List <Row> userFactorsRows = PackBatchOperatorUtil.unpackRows(modelRows, 0);
		List <Row> itemFactorsRows = PackBatchOperatorUtil.unpackRows(modelRows, 1);

		userFactors = new HashMap <>();
		itemFactors = new HashMap <>();

		userFactorsRows.forEach(row -> {
			userFactors.put(row.getField(0), VectorUtil.getDenseVector(row.getField(1)));
		});

		itemFactorsRows.forEach(row -> {
			itemFactors.put(row.getField(0), VectorUtil.getDenseVector(row.getField(1)));
		});

		if (excludeKnown) {
			List <Row> history = PackBatchOperatorUtil.unpackRows(modelRows, 2);
			historyItemUsers = new HashMap <>();
			historyUserItems = new HashMap <>();
			for (Row row : history) {
				Object user = row.getField(0);
				Object item = row.getField(1);
				if (historyUserItems.containsKey(user)) {
					historyUserItems.get(user).add(item);
				} else {
					HashSet <Object> items = new HashSet <>();
					items.add(item);
					historyUserItems.put(user, items);
				}
				if (historyItemUsers.containsKey(item)) {
					historyItemUsers.get(item).add(user);
				} else {
					HashSet <Object> users = new HashSet <>();
					users.add(user);
					historyItemUsers.put(item, users);
				}
			}
		}
	}

	@Override
	public Double rate(Row infoUserItem) throws Exception {
		return predictRating(infoUserItem, userColIdx, itemColIdx);
	}

	@Override
	public String recommendItemsPerUser(Row infoUser) throws Exception {
		Object userId = infoUser.getField(userColIdx);
		DenseVector userFea = userFactors.get(userId);
		//Preconditions.checkArgument(userFea != null, "can't find user id " + userId);
		Set <Object> excludes = null;
		if (excludeKnown) {
			excludes = historyUserItems.get(userId);
		}
		return recommend(userId, userFea, excludes, itemFactors, "rate");
	}

	@Override
	public String recommendUsersPerItem(Row infoItem) throws Exception {
		Object itemId = infoItem.getField(itemColIdx);
		DenseVector itemFea = itemFactors.get(itemId);
		//Preconditions.checkArgument(itemFea != null, "can't find item id " + itemId);
		Set <Object> excludes = null;
		if (excludeKnown) {
			excludes = historyItemUsers.get(itemId);
		}
		return recommend(itemId, itemFea, excludes, userFactors, "rate");
	}

	@Override
	public String recommendSimilarItems(Row infoItem) throws Exception {
		Object itemId = infoItem.getField(itemColIdx);
		DenseVector itemFea = itemFactors.get(itemId);
		//Preconditions.checkArgument(itemFea != null, "can't find item id " + itemId);
		Set <Object> excludes = new HashSet <>();
		excludes.add(itemId);
		return recommend(itemId, itemFea, excludes, itemFactors, "score");
	}

	@Override
	public String recommendSimilarUsers(Row infoUser) throws Exception {
		Object userId = infoUser.getField(userColIdx);
		DenseVector userFea = userFactors.get(userId);
		//Preconditions.checkArgument(userFea != null, "can't find user id " + userId);
		Set <Object> excludes = new HashSet <>();
		excludes.add(userId);
		return recommend(userId, userFea, excludes, userFactors, "score");
	}

	private Double predictRating(Row row, int userColIdx, int itemColIdx) throws Exception {
		Object userId = row.getField(userColIdx);
		Object itemId = row.getField(itemColIdx);
		DenseVector userFea = userFactors.get(userId);
		DenseVector itemFea = itemFactors.get(itemId);
		if (userFea != null && itemFea != null) {
			return BLAS.dot(userFea, itemFea);
		} else {
			return null; // unknown user or item
		}
	}

	private String recommend(Object userId, DenseVector userFea, Set <Object> excludes,
							 Map <Object, DenseVector> itemFeatures, String resultName) {
		RecommUtils.RecommPriorityQueue priorQueue = new RecommUtils.RecommPriorityQueue(topK);

		if (userFea != null) {
			itemFeatures.forEach((itemId, itemFea) -> {
				if (excludes == null || !excludes.contains(itemId)) {
					double score = BLAS.dot(userFea, itemFea);
					priorQueue.addOrReplace(itemId, score);
				}
			});
		}

		Tuple2 <List <Object>, List <Double>> itemsAndScores = priorQueue.getOrderedObjects();

		return KObjectUtil.serializeRecomm(
			KObjectUtil.OBJECT_NAME,
			itemsAndScores.f0,
			ImmutableMap.of(resultName, itemsAndScores.f1)
		);
	}
}
