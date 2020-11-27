package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp;
import com.alibaba.alink.operator.common.fm.FmModelMapper;
import com.alibaba.alink.operator.common.utils.PackBatchOperatorUtil;
import com.alibaba.alink.params.recommendation.BaseItemsPerUserRecommParams;
import com.alibaba.alink.params.recommendation.BaseRateRecommParams;
import com.alibaba.alink.params.recommendation.BaseUsersPerItemRecommParams;
import com.alibaba.alink.params.recommendation.FmPredictParams;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

final public class FmRecommKernel extends RecommKernel {

	private static final long serialVersionUID = 9054724969258810346L;
	protected transient FmModelMapper fmModelMapper;
	protected transient Map <Object, SparseVector> userFeatures;
	protected transient Map <Object, SparseVector> itemFeatures;
	protected transient Map <Object, Set <Object>> historyUserItems;
	protected transient Map <Object, Set <Object>> historyItemUsers;
	protected transient boolean isBinCls;

	private int userColIdx = -1;
	private int itemColIdx = -1;
	private Integer topK;
	private boolean excludeKnown = false;

	public FmRecommKernel(TableSchema modelSchema, TableSchema dataSchema, Params params, RecommType recommType) {
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
	Double rate(Row infoUserItem) throws Exception {
		Object userId = infoUserItem.getField(userColIdx);
		Object itemId = infoUserItem.getField(itemColIdx);
		SparseVector userFea = userFeatures.get(userId);
		SparseVector itemFea = itemFeatures.get(itemId);
		if (userFea != null && itemFea != null) {
			return getScore(combine(userFea, itemFea));
		} else {
			return null; // unknown user or item
		}
	}

	private String recommend(Object userId, SparseVector userFea, Set <Object> excludes,
							 Map <Object, SparseVector> itemFeatures, final boolean reverse) {
		RecommUtils.RecommPriorityQueue priorityQueue = new RecommUtils.RecommPriorityQueue(topK);

		if (userFea != null) {
			itemFeatures.forEach((itemId, itemFea) -> {
				if (excludes == null || !excludes.contains(itemId)) {
					float score = 0.F;
					try {
						SparseVector combined = combine(userFea, itemFea);
						if (reverse) {
							score = (float) getScore(combined);
						} else {
							score = (float) getScore(combined);
						}
					} catch (Exception e) {
						throw new RuntimeException(e);
					}

					priorityQueue.addOrReplace(itemId, score);
				}
			});
		}

		Tuple2 <List <Object>, List <Double>> itemsAndScores = priorityQueue.getOrderedObjects();

		return KObjectUtil.serializeRecomm(
			KObjectUtil.OBJECT_NAME,
			itemsAndScores.f0,
			ImmutableMap.of("rate", itemsAndScores.f1)
		);
	}

	@Override
	String recommendItemsPerUser(Row infoUser) throws Exception {
		Object userId = infoUser.getField(userColIdx);
		SparseVector userFea = userFeatures.get(userId);
		//Preconditions.checkArgument(userFea != null, "can't find feature id " + userId);
		Set <Object> excludes = null;
		if (excludeKnown) {
			excludes = historyUserItems.get(userId);
		}
		return recommend(userId, userFea, excludes, itemFeatures, false);
	}

	@Override
	public String recommendUsersPerItem(Row infoItem) throws Exception {
		Object itemId = infoItem.getField(itemColIdx);
		SparseVector itemFea = itemFeatures.get(itemId);
		//Preconditions.checkArgument(itemFea != null, "can't find feature id " + itemId);
		Set <Object> excludes = null;
		if (excludeKnown) {
			excludes = historyItemUsers.get(itemId);
		}
		return recommend(itemId, itemFea, excludes, userFeatures, true);
	}

	@Override
	String recommendSimilarItems(Row infoItem) throws Exception {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	String recommendSimilarUsers(Row infoUser) throws Exception {
		throw new UnsupportedOperationException("not supported");
	}

	protected double getScore(SparseVector feature) throws Exception {
		return fmModelMapper.getY(feature, isBinCls);
	}

	public static final TableSchema META_SCHEMA = new TableSchema(new String[] {"meta"},
		new TypeInformation[] {Types.STRING});

	@Override
	public void loadModel(List <Row> modelRows) {
		List <Row> fmModelRow = PackBatchOperatorUtil.unpackRows(modelRows, 0);
		fmModelMapper = new FmModelMapper(PackBatchOperatorUtil.unpackSchema(modelRows, getModelSchema(), 0),
			new TableSchema(new String[] {"__alink_features__"}, new TypeInformation[] {Types.STRING}),
			new Params().set(FmPredictParams.PREDICTION_COL, "prediction_result")
				.set(FmPredictParams.PREDICTION_DETAIL_COL, "prediction_detail")
				.set(FmPredictParams.RESERVED_COLS, new String[0]));
		fmModelMapper.loadModel(fmModelRow);
		isBinCls = fmModelMapper.getModel().task.equals(BaseFmTrainBatchOp.Task.BINARY_CLASSIFICATION);

		if (userColIdx >= 0) {
			Preconditions.checkArgument(
				PackBatchOperatorUtil.unpackSchema(modelRows, getModelSchema(), 1).getFieldTypes()[0].equals(super.getDataSchema().getFieldTypes()[userColIdx]),
				"user column type different from train set");
		}
		List <Row> userFeatureRows = PackBatchOperatorUtil.unpackRows(modelRows, 1);
		userFeatures = new HashMap <>();
		userFeatureRows.forEach(row -> {
			userFeatures.put(row.getField(0), VectorUtil.getSparseVector(row.getField(1)));
		});

		if (itemColIdx >= 0) {
			Preconditions.checkArgument(
				PackBatchOperatorUtil.unpackSchema(modelRows, getModelSchema(), 2).getFieldTypes()[0].equals(super.getDataSchema().getFieldTypes()[itemColIdx]),
				"user column type different from train set");
		}
		List <Row> itemFeatureRows = PackBatchOperatorUtil.unpackRows(modelRows, 2);
		itemFeatures = new HashMap <>();
		itemFeatureRows.forEach(row -> {
			itemFeatures.put(row.getField(0), VectorUtil.getSparseVector(row.getField(1)));
		});

		if (excludeKnown) {
			List <Row> history = PackBatchOperatorUtil.unpackRows(modelRows, 3);
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

	public static SparseVector combine(SparseVector v1, SparseVector v2) {
		Preconditions.checkArgument(v1.size() >= 0);
		Preconditions.checkArgument(v2.size() >= 0);
		int[] indices = new int[v1.getIndices().length + v2.getIndices().length];
		double[] values = new double[v1.getValues().length + v2.getValues().length];
		int size1 = v1.size();
		int size2 = v2.size();
		System.arraycopy(v1.getIndices(), 0, indices, 0, v1.getIndices().length);
		System.arraycopy(v1.getValues(), 0, values, 0, v1.getValues().length);
		System.arraycopy(v2.getIndices(), 0, indices, v1.getIndices().length, v2.getIndices().length);
		System.arraycopy(v2.getValues(), 0, values, v1.getValues().length, v2.getValues().length);
		int len1 = v1.getIndices().length;
		int len2 = v2.getIndices().length;
		for (int i = 0; i < len2; i++) {
			indices[len1 + i] += size1;
		}
		return new SparseVector(size1 + size2, indices, values);
	}
}
