package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;
import com.alibaba.alink.params.recommendation.BaseSimilarItemsRecommParams;

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
	private transient ThreadLocal <ItemCfRecommData> model;
	private Integer topN;
	private boolean excludeKnown = false;
	private transient ThreadLocal <double[]> scores;

	public ItemCfRecommKernel(TableSchema modelSchema, TableSchema dataSchema, Params params, RecommType recommType) {
		super(modelSchema, dataSchema, params, recommType);
		switch (recommType) {
			case SIMILAR_ITEMS: {
				this.topN = this.params.get(BaseSimilarItemsRecommParams.K);
				this.recommObjType = modelSchema.getFieldTypes()[1];
				break;
			}
			case ITEMS_PER_USER: {
				this.topN = this.params.get(BaseItemsPerUserRecommParams.K);
				this.excludeKnown = this.params.get(BaseItemsPerUserRecommParams.EXCLUDE_KNOWN);
				this.recommObjType = modelSchema.getFieldTypes()[1];
				break;
			}
			case RATE: {
				break;
			}
			case USERS_PER_ITEM: {
				this.topN = this.params.get(BaseItemsPerUserRecommParams.K);
				this.excludeKnown = this.params.get(BaseItemsPerUserRecommParams.EXCLUDE_KNOWN);
				this.recommObjType = modelSchema.getFieldTypes()[0];
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
			} else if (d > head) {
				RecommItemTopKResult peek = map.poll();
				peek.similarity = d;
				peek.item = obj;
				map.add(peek);
				head = map.peek().similarity;
			}

		}
		return head;
	}

	static MTable recommendItems(Object userId,
								 ItemCfRecommData model,
								 int topN,
								 boolean excludeKnown,
								 double[] res,
								 String objectName,
								 TypeInformation <?> objType) {
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

		return serializeQueue(queue, KObjectUtil.SCORE_NAME, objectName, objType);
	}

	static MTable recommendUsers(Object itemId,
								 ItemCfRecommData model,
								 int topN,
								 boolean excludeKnown,
								 String objectName,
								 TypeInformation <?> objType) {
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
					res.merge(t.f0, t.f1 * value[i], Double::sum);
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

		return serializeQueue(queue, KObjectUtil.SCORE_NAME, objectName, objType);
	}

	static MTable findSimilarItems(Object itemId,
								   ItemCfRecommData model,
								   int topN,
								   String objectname,
								   TypeInformation <?> objType) {
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
		return serializeQueue(queue, "similarities", objectname, objType);
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
		for (Row row : modelRows) {
			if (row.getField(0) == null && row.getField(1) == null) {
				Params params = Params.fromJson((String) row.getField(2));
				userColName = params.getString("userCol");
				itemColName = params.getString("itemCol");
			}
		}
		model = ThreadLocal.withInitial(() -> new ItemCfRecommModelDataConverter(recommType).load(modelRows));
		scores = ThreadLocal.withInitial(() -> new double[model.get().items.length]);
	}

	@Override
	public Double rate(Object[] ids) {
		Object userId = ids[0];
		Object itemId = ids[1];
		return rate(userId, itemId, model.get());
	}

	@Override
	public MTable recommendItemsPerUser(Object userId) {
		return recommendItems(userId, model.get(), topN, excludeKnown, scores.get(), itemColName, recommObjType);
	}

	@Override
	public MTable recommendUsersPerItem(Object itemId) {
		return recommendUsers(itemId, model.get(), topN, excludeKnown, userColName, recommObjType);
	}

	@Override
	public MTable recommendSimilarItems(Object itemId) {
		return findSimilarItems(itemId, model.get(), topN, itemColName, recommObjType);
	}

	private static MTable serializeQueue(Queue <RecommItemTopKResult> queue, String key, String objectName,
										 TypeInformation <?> objType) {
		List <Row> rows = new ArrayList <>();
		while (!queue.isEmpty()) {
			RecommItemTopKResult result = queue.poll();
			rows.add(Row.of(result.item, result.similarity));
		}
		Collections.reverse(rows);
		return new MTable(rows, objectName + " " + FlinkTypeConverter.getTypeString(objType) + "," + key + " DOUBLE");
	}

	@Override
	public MTable recommendSimilarUsers(Object userId) {
		throw new RuntimeException("ItemCf not support recommendSimilarUsers");
	}
}
