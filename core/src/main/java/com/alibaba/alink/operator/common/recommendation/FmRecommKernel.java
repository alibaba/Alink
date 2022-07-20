package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp;
import com.alibaba.alink.operator.common.fm.FmModelMapper;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
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
	private transient FmModelMapper fmModelMapper;
	private transient Map <Object, SparseVector> userFeatures;
	private transient Map <Object, SparseVector> itemFeatures;
	private transient Map <Object, Set <Object>> historyUserItems;
	private transient Map <Object, Set <Object>> historyItemUsers;
	private transient boolean isBinCls;

	private int userColIdx = -1;
	private int itemColIdx = -1;
	private final Integer topK;
	private boolean excludeKnown = false;

	public FmRecommKernel(TableSchema modelSchema, TableSchema dataSchema, Params params, RecommType recommType) {
		super(modelSchema, dataSchema, params, recommType);
		userColName = getParamDefaultAsNull(params, BaseRateRecommParams.USER_COL);
		itemColName = getParamDefaultAsNull(params, BaseRateRecommParams.ITEM_COL);
		this.topK = getParamDefaultAsNull(params, BaseItemsPerUserRecommParams.K);
		if (userColName != null) {
			this.userColIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), userColName);
		}
		if (itemColName != null) {
			this.itemColIdx = TableUtil.findColIndex(dataSchema.getFieldNames(), itemColName);
		}

		if (recommType == RecommType.RATE) {
			AkPreconditions.checkArgument(userColIdx >= 0,
				new AkIllegalArgumentException("Can't find user col: " + userColName));
			AkPreconditions.checkArgument(itemColIdx >= 0,
				new AkIllegalArgumentException("Can't find item col: " + itemColName));
		} else if (recommType == RecommType.ITEMS_PER_USER) {
			AkPreconditions.checkArgument(userColIdx >= 0,
				new AkIllegalArgumentException("Can't find user col: " + userColName));
			AkPreconditions.checkArgument(topK != null, new AkIllegalArgumentException("Missing param topK"));
			excludeKnown = params.get(BaseItemsPerUserRecommParams.EXCLUDE_KNOWN);
		} else if (recommType == RecommType.USERS_PER_ITEM) {
			AkPreconditions.checkArgument(itemColIdx >= 0,
				new AkIllegalArgumentException("Can't find item col: " + itemColName));
			AkPreconditions.checkArgument(topK != null, new AkIllegalArgumentException("Missing param topK"));
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
	Double rate(Object[] ids) {
		Object userId = ids[0];
		Object itemId = ids[1];
		SparseVector userFea = userFeatures.get(userId);
		SparseVector itemFea = itemFeatures.get(itemId);
		if (userFea != null && itemFea != null) {
			return getScore(combine(userFea, itemFea));
		} else {
			/* unknown user or item */
			return null;
		}
	}

	private MTable recommend(String objectColName, SparseVector userFea, Set <Object> excludes,
							 Map <Object, SparseVector> itemFeatures, final boolean reverse) {
		RecommUtils.RecommPriorityQueue priorityQueue = new RecommUtils.RecommPriorityQueue(topK);

		if (userFea != null) {
			itemFeatures.forEach((itemId, itemFea) -> {
				if (excludes == null || !excludes.contains(itemId)) {
					float score;
					try {
						SparseVector combined = combine(userFea, itemFea);
						if (reverse) {
							score = (float) getScore(combined);
						} else {
							score = (float) getScore(combined);
						}
					} catch (Exception e) {
						throw new AkUnclassifiedErrorException(e.toString());
					}

					priorityQueue.addOrReplace(itemId, score);
				}
			});
		}

		List <Row> rows = priorityQueue.getOrderedRows();
		return new MTable(rows,
			objectColName + " " + FlinkTypeConverter.getTypeString(recommObjType) + "," + KObjectUtil.RATING_NAME
				+ " DOUBLE");
	}

	@Override
	public MTable recommendItemsPerUser(Object userId) {
		SparseVector userFea = userFeatures.get(userId);
		Set <Object> excludes = null;
		if (excludeKnown) {
			excludes = historyUserItems.get(userId);
		}
		return recommend(itemColName, userFea, excludes, itemFeatures, false);
	}

	@Override
	public MTable recommendUsersPerItem(Object itemId) {
		SparseVector itemFea = itemFeatures.get(itemId);
		Set <Object> excludes = null;
		if (excludeKnown) {
			excludes = historyItemUsers.get(itemId);
		}
		return recommend(userColName, itemFea, excludes, userFeatures, true);
	}

	@Override
	public MTable recommendSimilarItems(Object itemId) {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	public MTable recommendSimilarUsers(Object userId) {
		throw new UnsupportedOperationException("not supported");
	}

	protected double getScore(SparseVector feature) {
		return fmModelMapper.getY(feature, isBinCls);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		for (Row row : modelRows) {
			if (((Number) row.getField(0)).intValue() == -1) {
				Tuple2 <List <List <String>>, List <List <Integer>>> metaData =
					JsonConverter.fromJson((String) row.getField(1), Tuple2.class);
				userColName = metaData.f0.get(3).get(0);
				itemColName = metaData.f0.get(3).get(1);
				int userIdx = metaData.f1.get(3).get(0);
				int itemIdx = metaData.f1.get(3).get(1);
				if (recommType == RecommType.ITEMS_PER_USER) {
					recommObjType = getModelSchema().getFieldTypes()[itemIdx];
				} else if (recommType == RecommType.USERS_PER_ITEM) {
					recommObjType = getModelSchema().getFieldTypes()[userIdx];
				} else if (recommType == RecommType.SIMILAR_USERS) {
					recommObjType = getModelSchema().getFieldTypes()[userIdx];
				} else if (recommType == RecommType.SIMILAR_ITEMS) {
					recommObjType = getModelSchema().getFieldTypes()[itemIdx];
				}
			}
		}

		List <Row> fmModelRow = PackBatchOperatorUtil.unpackRows(modelRows, 0);
		fmModelMapper = new FmModelMapper(PackBatchOperatorUtil.unpackSchema(modelRows, getModelSchema(), 0),
			new TableSchema(new String[] {"__alink_features__"}, new TypeInformation[] {Types.STRING}),
			new Params().set(FmPredictParams.PREDICTION_COL, "prediction_result")
				.set(FmPredictParams.PREDICTION_DETAIL_COL, "prediction_detail")
				.set(FmPredictParams.RESERVED_COLS, new String[0]));
		fmModelMapper.loadModel(fmModelRow);
		isBinCls = fmModelMapper.getModel().task.equals(BaseFmTrainBatchOp.Task.BINARY_CLASSIFICATION);

		if (userColIdx >= 0) {
			AkPreconditions.checkArgument(
				PackBatchOperatorUtil.unpackSchema(modelRows, getModelSchema(), 1).getFieldTypes()[0]
					.equals(super.getDataSchema().getFieldTypes()[userColIdx]),
				new AkIllegalDataException("user column type different from train set"));
		}
		List <Row> userFeatureRows = PackBatchOperatorUtil.unpackRows(modelRows, 1);
		userFeatures = new HashMap <>();
		userFeatureRows.forEach(row -> userFeatures.put(row.getField(0), VectorUtil.getSparseVector(row.getField(1))));

		if (itemColIdx >= 0) {
			AkPreconditions.checkArgument(
				PackBatchOperatorUtil.unpackSchema(modelRows, getModelSchema(), 2).getFieldTypes()[0]
					.equals(super.getDataSchema().getFieldTypes()[itemColIdx]),
				new AkIllegalDataException("user column type different from train set"));
		}
		List <Row> itemFeatureRows = PackBatchOperatorUtil.unpackRows(modelRows, 2);
		itemFeatures = new HashMap <>();
		itemFeatureRows.forEach(row -> itemFeatures.put(row.getField(0), VectorUtil.getSparseVector(row.getField(1))));

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
		AkPreconditions.checkArgument(v1.size() >= 0);
		AkPreconditions.checkArgument(v2.size() >= 0);
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

	@Override
	public RecommKernel createNew() {
		return new FmRecommKernel(getModelSchema(), getDataSchema(), params.clone(), recommType);
	}
}
