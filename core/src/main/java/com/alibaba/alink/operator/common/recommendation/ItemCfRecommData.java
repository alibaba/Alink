package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.SparseVector;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * recommendation data for ItemSimilarityRecommendation, different recommendtype have different data.
 */
public class ItemCfRecommData implements Serializable {
	private static final long serialVersionUID = 4715290931520980072L;

	/**
	 * Map key: user
	 * Map item: SparseVector(key: item id, value: rate)
	 * Used in RATE and ITEMS_PER_USER type.
	 */
	Map <Object, SparseVector> userItemRates;

	/**
	 * Map key: item id
	 * Map item: SparseVector(key: neighbor item id, value: similarity)
	 * Used in RATE, SIMILAR_USERS, SIMILAR_ITEMS, USERS_PER_ITEM type.
	 */
	Map <Integer, SparseVector> itemSimilarities;

	/**
	 * Item id is continuous, so the length of itemSimilarityList is the number of distinct items.
	 * The index in the itemSimilarityList is the corresponding item id.
	 * List<Tuple2<Integer, Double>> contains the <ItemId, similarity> pair for an item.
	 *
	 * Used in ITEMS_PER_USER.
	 */
	List <Tuple2 <Integer, Double>>[] itemSimilarityList;

	/**
	 * Item id is continuous, so the length of itemSimilarityList is the number of distinct items.
	 * The index in the itemSimilarityList is the corresponding item id.
	 * List<Tuple2<Integer, Double>> contains the <User, rate> pair for an item.
	 *
	 * Used in USERS_PER_ITEM.
	 */
	List <Tuple2 <Object, Double>>[] userRateList;

	/**
	 * Map key: user
	 * Map item: set of the items the user rate.
	 * Used in ITEMS_PER_USER.
	 */
	Map <Object, Set <Integer>> userItems;

	/**
	 * Map key: item id.
	 * Map item: set of the users who rate the item.
	 * Used in USERS_PER_ITEM.
	 */
	Map <Integer, Set <Object>> itemUsers;

	/**
	 * Item List.
	 * Used in SIMILAR_USERS, SIMILAR_ITEMS, RATE, ITEMS_PER_USER.
	 */
	Object[] items;

	/**
	 * Map key: item.
	 * Map item: item id.
	 * Used in SIMILAR_USERS, SIMILAR_ITEMS, RATE, USERS_PER_ITEM.
	 */
	Map <Object, Integer> itemMap;

	/**
	 * rate column name, it could be null.
	 */
	String rateCol;

	/**
	 * Some meta information.
	 */
	Params meta;
}
