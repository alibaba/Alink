package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;
import com.alibaba.alink.params.recommendation.BaseRateRecommParams;
import com.alibaba.alink.params.recommendation.BaseSimilarItemsRecommParams;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

public class ItemCfRecommKernel extends RecommKernel implements Cloneable {
	private static final long serialVersionUID = 9200579594017986392L;
	private ItemCfRecommData model = null;
	private int userColIdx = -1;
	private int itemColIdx = -1;
	private Integer topN;
	private boolean excludeKnown = false;
	private double[] scores = null;

	public ItemCfRecommKernel(TableSchema modelSchema, TableSchema dataSchema, Params params, RecommType recommType) {
		super(modelSchema, dataSchema, params, recommType);
		switch (recommType) {
			case SIMILAR_ITEMS: {
				String itemColName = this.params.get(BaseSimilarItemsRecommParams.ITEM_COL);
				this.itemColIdx = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), itemColName);
				this.topN = this.params.get(BaseSimilarItemsRecommParams.K);
				break;
			}
			case ITEMS_PER_USER: {
				String userColName = this.params.get(BaseItemsPerUserRecommParams.USER_COL);
				this.userColIdx = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), userColName);
				this.topN = this.params.get(BaseItemsPerUserRecommParams.K);
				this.excludeKnown = this.params.get(BaseItemsPerUserRecommParams.EXCLUDE_KNOWN);
				break;
			}
			case RATE: {
				String itemColName = this.params.get(BaseRateRecommParams.ITEM_COL);
				this.itemColIdx = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), itemColName);
				String userColName = this.params.get(BaseRateRecommParams.USER_COL);
				this.userColIdx = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), userColName);
				break;
			}
			case USERS_PER_ITEM: {
				String itemColName = this.params.get(BaseUsersPerItemRecommParams.ITEM_COL);
				this.itemColIdx = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), itemColName);
				this.topN = this.params.get(BaseItemsPerUserRecommParams.K);
				this.excludeKnown = this.params.get(BaseItemsPerUserRecommParams.EXCLUDE_KNOWN);
				break;
			}
			default: {
				throw new RuntimeException("ItemKnn not support " + recommType + " yet!");
			}
		}
	}

	public static class RecommItemTopKResult implements Serializable {
		private static final long serialVersionUID = -4530403725552300088L;
		public Object item;
		public Double similarity;

		public RecommItemTopKResult() {
		}

		public RecommItemTopKResult(Object item, Double similarity) {
			this.item = item;
			this.similarity = similarity;
		}
	}

	private static double updateQueue(Queue <RecommItemTopKResult> map, int topN, Double d, Object obj, double head) {
		if (d != null) {
			if (map.size() < topN) {
				map.add(new RecommItemTopKResult(obj, d));
				head = map.peek().similarity;
			} else {
				if (d > head) {
					RecommItemTopKResult peek = map.poll();
					peek.similarity = d;
					peek.item = obj;
					map.add(peek);
					head = map.peek().similarity;
				}
			}
		}
		return head;
	}

	static String recommendItems(Object userId,
								 ItemCfRecommData model,
								 int topN,
								 boolean excludeKnown,
								 double[] res) {
		Arrays.fill(res, 0.0);
		PriorityQueue <RecommItemTopKResult> queue = new PriorityQueue <>(Comparator.comparing(o -> o.similarity));
		SparseVector itemRate = model.userItemRates.get(userId);
		if (null == itemRate) {
			return null;
		}
		Set <Integer> items = model.userItems.get(userId);
		int[] key = itemRate.getIndices();
		double[] value = itemRate.getValues();
		for (int i = 0; i < key.length; i++) {
			if (model.itemSimilarityList[key[i]] != null) {
				for (Tuple2 <Integer, Double> t : model.itemSimilarityList[key[i]]) {
					res[t.f0] += t.f1 * value[i];
				}
			}
		}
		double head = 0;
		for (int i = 0; i < res.length; i++) {
			if (excludeKnown && items.contains(i)) {
				continue;
			}
			head = updateQueue(queue, topN, res[i] / items.size(), model.items[i], head);
		}

		return serializeQueue(queue, "score");
	}

	static String recommendUsers(Object itemId,
								 ItemCfRecommData model,
								 int topN,
								 boolean excludeKnown) {
		PriorityQueue <RecommItemTopKResult> queue = new PriorityQueue <>(Comparator.comparing(o -> o.similarity));
		Integer itemIndex = model.itemMap.get(itemId);
		if (null == itemIndex) {
			return null;
		}
		SparseVector itemSimilarity = model.itemSimilarities.get(itemIndex);
		Set <Object> users = model.itemUsers.get(itemIndex);
		int[] key = itemSimilarity.getIndices();
		double[] value = itemSimilarity.getValues();
		Map <Object, Double> res = new HashMap <>();
		for (int i = 0; i < key.length; i++) {
			if (model.userRateList[key[i]] != null) {
				for (Tuple2 <Object, Double> t : model.userRateList[key[i]]) {
					res.merge(t.f0, t.f1 * value[i], (v1, v2) -> (v1 + v2));
				}
			}
		}
		double head = 0;
		for (Map.Entry <Object, Double> entry : res.entrySet()) {
			if (excludeKnown && users.contains(entry.getKey())) {
				continue;
			}
			head = updateQueue(queue, topN, entry.getValue() / users.size(), entry.getKey(), head);
		}

		return serializeQueue(queue, "score");
	}

	static String findSimilarItems(Object itemId,
								   ItemCfRecommData model,
								   int topN) {
		PriorityQueue <RecommItemTopKResult> queue = new PriorityQueue <>(Comparator.comparing(o -> o.similarity));
		Integer itemIndex = model.itemMap.get(itemId);
		if (null == itemIndex) {
			return null;
		}
		SparseVector itemSimilarity = model.itemSimilarities.get(itemIndex);
		int[] key = itemSimilarity.getIndices();
		double[] value = itemSimilarity.getValues();
		double head = 0;
		for (int i = 0; i < key.length; i++) {
			head = updateQueue(queue, topN, value[i], model.items[key[i]], head);
		}
		return serializeQueue(queue, "similarities");
	}

	static Double rate(Object userId, Object itemId, ItemCfRecommData model) {
		SparseVector userItem = model.userItemRates.get(userId);
		Integer itemIndex = model.itemMap.get(itemId);
		if (null == userItem || null == itemIndex) {
			return null;
		}
		SparseVector itemSimilarity = model.itemSimilarities.get(itemIndex);
		double dot = 0;
		double similarity = 0;
		int p0 = 0;
		int p1 = 0;
		double[] userItemsValues = userItem.getValues();
		int[] userItemsIndices = userItem.getIndices();
		double[] itemSimilarityValues = itemSimilarity.getValues();
		int[] itemSimilarityIndices = itemSimilarity.getIndices();
		while (p0 < userItemsValues.length && p1 < itemSimilarityValues.length) {
			if (userItemsIndices[p0] == itemSimilarityIndices[p1]) {
				dot += userItemsValues[p0] * itemSimilarityValues[p1];
				similarity += itemSimilarityValues[p1];
				p0++;
				p1++;
			} else if (userItemsIndices[p0] < itemSimilarityIndices[p1]) {
				p0++;
			} else {
				p1++;
			}
		}
		return model.rateCol == null ? (dot / userItem.numberOfValues()) : (similarity == 0 ? 0.0 : dot / similarity);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		this.model = new ItemCfRecommModelDataConverter(recommType).load(modelRows);
	}

	@Override
	public Double rate(Row infoUserItem) {
		Object userId = infoUserItem.getField(userColIdx);
		Object itemId = infoUserItem.getField(itemColIdx);
		return rate(userId, itemId, model);
	}

	@Override
	public String recommendItemsPerUser(Row infoUser) {
		Object userId = infoUser.getField(userColIdx);
		if (null == scores) {
			scores = new double[model.items.length];
		}
		return recommendItems(userId, model, topN, excludeKnown, scores);
	}

	@Override
	public String recommendUsersPerItem(Row infoItem) {
		Object itemId = infoItem.getField(itemColIdx);
		return recommendUsers(itemId, model, topN, excludeKnown);
	}

	@Override
	public String recommendSimilarItems(Row infoItem) {
		Object itemId = infoItem.getField(itemColIdx);
		return findSimilarItems(itemId, model, topN);
	}

	private static String serializeQueue(Queue <RecommItemTopKResult> queue, String key) {
		List <Object> items = new ArrayList <>();
		List <Double> similarity = new ArrayList <>();
		while (!queue.isEmpty()) {
			RecommItemTopKResult result = queue.poll();
			items.add(result.item);
			similarity.add(result.similarity);
		}
		Collections.reverse(items);
		Collections.reverse(similarity);
		return KObjectUtil.serializeRecomm(
			KObjectUtil.OBJECT_NAME,
			items,
			ImmutableMap.of(key, similarity)
		);
	}

	@Override
	public String recommendSimilarUsers(Row infoUser) {
		throw new RuntimeException("ItemCf not support recommendSimilarUsers");
	}

	@Override
	protected RecommKernel mirror() {
		try {
			ItemCfRecommKernel kernel = (ItemCfRecommKernel) this.clone();
			return kernel;
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}

}
