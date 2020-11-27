package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.model.ModelDataConverter;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.params.recommendation.ItemCfRecommTrainParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ModelDataConverter for ItemSimilarityRecommendation.
 */
public class ItemCfRecommModelDataConverter
	implements ModelDataConverter <Tuple2 <Params, Iterable <Row>>, ItemCfRecommData> {
	private String userCol;
	private String itemCol;
	private TypeInformation userType;
	private RecommType recommType;

	public static ParamInfo <String[]> ITEMS = ParamInfoFactory
		.createParamInfo("items", String[].class)
		.setDescription("items lists")
		.setRequired()
		.build();

	public static ParamInfo <String> ITEM_TYPE = ParamInfoFactory
		.createParamInfo("itemType", String.class)
		.setDescription("itemType")
		.setRequired()
		.build();

	public static ParamInfo <String> USER_TYPE = ParamInfoFactory
		.createParamInfo("userType", String.class)
		.setDescription("userType")
		.setRequired()
		.build();

	public ItemCfRecommModelDataConverter(String userCol, TypeInformation userType, String itemCol) {
		this.userCol = userCol;
		this.itemCol = itemCol;
		this.userType = userType;
	}

	public ItemCfRecommModelDataConverter(RecommType recommType) {
		this.recommType = recommType;
	}

	@Override
	public TableSchema getModelSchema() {
		String[] colNames = new String[] {userCol, itemCol, "vector"};
		TypeInformation[] colTypes = new TypeInformation[] {userType, Types.LONG, Types.STRING};
		return new TableSchema(colNames, colTypes);
	}

	@Override
	public ItemCfRecommData load(List <Row> rows) {
		ItemCfRecommData modelData = new ItemCfRecommData();
		switch (recommType) {
			case USERS_PER_ITEM: {
				String[] items;
				modelData.itemSimilarities = new HashMap <>();
				for (Row row : rows) {
					if (row.getField(0) != null) {
						Object userId = row.getField(0);
						SparseVector vector = VectorUtil.getSparseVector(row.getField(2));
						if (modelData.userRateList == null) {
							modelData.userRateList = new List[vector.size()];
						}
						double[] value = vector.getValues();
						int[] key = vector.getIndices();
						for (int j = 0; j < key.length; j++) {
							if (modelData.userRateList[key[j]] == null) {
								modelData.userRateList[key[j]] = new ArrayList <>();
							}
							modelData.userRateList[key[j]].add(Tuple2.of(userId, value[j]));
						}
					} else if (row.getField(1) != null) {
						modelData.itemSimilarities.put(((Number) row.getField(1)).intValue(),
							VectorUtil.getSparseVector(row.getField(2)));
					} else {
						modelData.meta = Params.fromJson((String) row.getField(2));
						items = modelData.meta.get(ITEMS);
						TypeInformation itemType = FlinkTypeConverter.getFlinkType(modelData.meta.get(ITEM_TYPE));
						modelData.itemMap = new HashMap <>();
						for (int i = 0; i < items.length; i++) {
							modelData.itemMap.put(EvaluationUtil.castTo(items[i], itemType), i);
						}
						modelData.rateCol = modelData.meta.get(ItemCfRecommTrainParams.RATE_COL);
					}
				}
				modelData.itemUsers = new HashMap <>();
				for (int i = 0; i < modelData.userRateList.length; i++) {
					Set <Object> users = new HashSet <>();
					for (Tuple2 <Object, Double> t : modelData.userRateList[i]) {
						users.add(t.f0);
					}
					modelData.itemUsers.put(i, users);
				}
				break;
			}
			case SIMILAR_ITEMS:
			case SIMILAR_USERS: {
				modelData.itemSimilarities = new HashMap <>();
				for (Row row : rows) {
					if (row.getField(1) != null) {
						modelData.itemSimilarities.put(((Number) row.getField(1)).intValue(),
							VectorUtil.getSparseVector(row.getField(2)));
					} else if (row.getField(0) == null) {
						modelData.meta = Params.fromJson((String) row.getField(2));
						String[] items = modelData.meta.get(ITEMS);
						modelData.items = new Object[items.length];
						TypeInformation itemType = FlinkTypeConverter.getFlinkType(modelData.meta.get(ITEM_TYPE));
						modelData.itemMap = new HashMap <>();
						for (int i = 0; i < items.length; i++) {
							modelData.items[i] = EvaluationUtil.castTo(items[i], itemType);
							modelData.itemMap.put(modelData.items[i], i);
						}
						modelData.rateCol = modelData.meta.get(ItemCfRecommTrainParams.RATE_COL);
					}
				}
				break;
			}
			case ITEMS_PER_USER: {
				modelData.userItemRates = new HashMap <>();
				for (Row row : rows) {
					if (row.getField(0) != null) {
						modelData.userItemRates.put(row.getField(0), VectorUtil.getSparseVector(row.getField(2)));
					} else if (row.getField(1) != null) {
						Integer itemId = ((Number) row.getField(1)).intValue();
						SparseVector vector = VectorUtil.getSparseVector(row.getField(2));
						if (modelData.itemSimilarityList == null) {
							modelData.itemSimilarityList = new List[vector.size()];
						}
						double[] value = vector.getValues();
						int[] key = vector.getIndices();
						for (int j = 0; j < key.length; j++) {
							if (modelData.itemSimilarityList[key[j]] == null) {
								modelData.itemSimilarityList[key[j]] = new ArrayList <>();
							}
							modelData.itemSimilarityList[key[j]].add(Tuple2.of(itemId, value[j]));
						}
					} else {
						modelData.meta = Params.fromJson((String) row.getField(2));
						String[] items = modelData.meta.get(ITEMS);
						modelData.items = new Object[items.length];
						TypeInformation itemType = FlinkTypeConverter.getFlinkType(modelData.meta.get(ITEM_TYPE));
						for (int i = 0; i < items.length; i++) {
							modelData.items[i] = EvaluationUtil.castTo(items[i], itemType);
						}
						modelData.rateCol = modelData.meta.get(ItemCfRecommTrainParams.RATE_COL);
					}
				}
				modelData.userItems = new HashMap <>();
				for (Map.Entry <Object, SparseVector> entry : modelData.userItemRates.entrySet()) {
					Set <Integer> items = new HashSet <>();
					for (int key : entry.getValue().getIndices()) {
						items.add(key);
					}
					modelData.userItems.put(entry.getKey(), items);
				}
				break;
			}
			case RATE: {
				modelData.userItemRates = new HashMap <>();
				modelData.itemSimilarities = new HashMap <>();
				for (Row row : rows) {
					if (row.getField(0) != null) {
						modelData.userItemRates.put(row.getField(0), VectorUtil.getSparseVector(row.getField(2)));
					} else if (row.getField(1) != null) {
						modelData.itemSimilarities.put(((Number) row.getField(1)).intValue(),
							VectorUtil.getSparseVector(row.getField(2)));
					} else {
						modelData.meta = Params.fromJson((String) row.getField(2));
						modelData.rateCol = modelData.meta.get(ItemCfRecommTrainParams.RATE_COL);
						String[] items = modelData.meta.get(ITEMS);
						modelData.items = new Object[items.length];
						TypeInformation itemType = FlinkTypeConverter.getFlinkType(modelData.meta.get(ITEM_TYPE));
						modelData.itemMap = new HashMap <>();
						for (int i = 0; i < items.length; i++) {
							modelData.items[i] = EvaluationUtil.castTo(items[i], itemType);
							modelData.itemMap.put(modelData.items[i], i);
						}
					}
				}
				break;
			}
			default: {
				throw new RuntimeException("Not support yet!");
			}
		}
		return modelData;
	}

	@Override
	public void save(Tuple2 <Params, Iterable <Row>> modelData, Collector <Row> collector) {
		if (modelData.f0 != null) {
			collector.collect(Row.of(null, null, modelData.f0.toJson()));
		}
		modelData.f1.forEach(collector::collect);
	}
}
