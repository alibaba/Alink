package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.OneHotPredictBatchOp;
import com.alibaba.alink.operator.batch.feature.OneHotTrainBatchOp;
import com.alibaba.alink.operator.batch.similarity.VectorNearestNeighborPredictBatchOp;
import com.alibaba.alink.operator.batch.similarity.VectorNearestNeighborTrainBatchOp;
import com.alibaba.alink.operator.batch.source.DataSetWrapperBatchOp;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelData;
import com.alibaba.alink.operator.common.feature.OneHotModelDataConverter;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.recommendation.ItemCfModelInfo;
import com.alibaba.alink.operator.common.recommendation.ItemCfRecommModelDataConverter;
import com.alibaba.alink.operator.common.similarity.NearestNeighborsMapper;
import com.alibaba.alink.params.feature.HasEncodeWithoutWoe;
import com.alibaba.alink.params.recommendation.ItemCfRecommTrainParams;
import com.alibaba.alink.params.shared.clustering.HasFastMetric;

import java.util.List;
import java.util.TreeMap;

/**
 * A model that ranks an item according to its calc to other items observed for the user in question.
 */
public class ItemCfTrainBatchOp extends BatchOperator <ItemCfTrainBatchOp>
	implements ItemCfRecommTrainParams <ItemCfTrainBatchOp>,
	WithModelInfoBatchOp <ItemCfModelInfo, ItemCfTrainBatchOp, ItemCfModelInfoBatchOp> {

	private static final long serialVersionUID = -5873113492724718667L;
	private static String USER_NUM = "userNum";
	private static String[] COL_NAMES = new String[] {"itemId", "itemVector"};
	private static String USER_ENCODE = "userEncode";

	public ItemCfTrainBatchOp() {
		super(new Params());
	}

	public ItemCfTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public ItemCfTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		final String userCol = getUserCol();
		final String itemCol = getItemCol();
		final String rateCol = getRateCol();
		final TypeInformation userType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), userCol);
		final String itemType = FlinkTypeConverter.getTypeString(
			TableUtil.findColTypeWithAssertAndHint(in.getSchema(), itemCol));

		if (null == rateCol) {
			Preconditions.checkArgument(getSimilarityType().equals(SimilarityType.JACCARD),
				"When rateCol is not given, only Jaccard calc is supported!");
		}

		String[] selectedCols = (null == rateCol ? new String[] {userCol, itemCol}
			: new String[] {userCol, itemCol, rateCol});
		in = in.select(selectedCols);

		OneHotTrainBatchOp oneHot = new OneHotTrainBatchOp()
			.setSelectedCols(userCol, itemCol)
			.linkFrom(in);

		BatchOperator userEncode = new OneHotPredictBatchOp()
			.setSelectedCols(userCol, itemCol)
			.setOutputCols(USER_ENCODE, itemCol)
			.setEncode(HasEncodeWithoutWoe.Encode.INDEX)
			.linkFrom(oneHot, in);

		DataSet <Row> itemVector = userEncode
			.select(
				null == rateCol ? new String[] {USER_ENCODE, itemCol} : new String[] {USER_ENCODE, itemCol, rateCol})
			.getDataSet()
			.groupBy(1)
			.reduceGroup(new ItemVectorGenerator(rateCol, userCol))
			.withBroadcastSet(oneHot.getSideOutput(0).getDataSet(), USER_NUM)
			.name("GenerateItemVector");

		DataSet <Row> userVector = userEncode
			.select(null == rateCol ? new String[] {userCol, itemCol} : new String[] {userCol, itemCol, rateCol})
			.getDataSet()
			.groupBy(0)
			.reduceGroup(new UserItemVectorGenerator(rateCol, itemCol))
			.withBroadcastSet(oneHot.getSideOutput(0).getDataSet(), USER_NUM)
			.name("GetUserItems");

		BatchOperator items = new DataSetWrapperBatchOp(itemVector, COL_NAMES,
			new TypeInformation[] {Types.LONG, VectorTypes.SPARSE_VECTOR});

		BatchOperator train = new VectorNearestNeighborTrainBatchOp()
			.setIdCol(COL_NAMES[0])
			.setSelectedCol(COL_NAMES[1])
			.setMetric(HasFastMetric.Metric.valueOf(this.getSimilarityType().name()))
			.linkFrom(items);

		BatchOperator op = new VectorNearestNeighborPredictBatchOp()
			.setSelectedCol(COL_NAMES[1])
			.setReservedCols(COL_NAMES[0])
			.setTopN(this.getMaxNeighborNumber() + 1)
			.setRadius(1.0 - this.getSimilarityThreshold())
			.linkFrom(train, items);

		DataSet <Row> itemSimilarities = op
			.select(new String[] {COL_NAMES[0], COL_NAMES[1]})
			.getDataSet()
			.mapPartition(new ItemSimilarityVectorGenerator(itemCol))
			.withBroadcastSet(oneHot.getSideOutput(0).getDataSet(), USER_NUM)
			.name("CalcItemSimilarity");

		DataSet <Row> itemMapTable = oneHot.getDataSet()
			.filter(new FilterFunction <Row>() {
				private static final long serialVersionUID = 7406134775433418651L;

				@Override
				public boolean filter(Row value) throws Exception {
					return !value.getField(0).equals(0L);
				}
			});

		Params params = getParams();

		DataSet <Row> outs = userVector.union(itemSimilarities)
			.mapPartition(new RichMapPartitionFunction <Row, Row>() {
				private static final long serialVersionUID = 3779020277896699637L;

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Row> out) {
					Params meta = null;
					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						List <Row> modelRows = getRuntimeContext().getBroadcastVariable("ITEM_MAP");
						MultiStringIndexerModelData modelData = new OneHotModelDataConverter().load(
							modelRows).modelData;
						String[] items = new String[(int) modelData.getNumberOfTokensOfColumn(itemCol)];
						for (int i = 0; i < items.length; i++) {
							items[i] = modelData.getToken(itemCol, (long) i);
						}
						meta = params.set(ItemCfRecommTrainParams.RATE_COL, rateCol)
							.set(ItemCfRecommModelDataConverter.ITEMS, items)
							.set(ItemCfRecommModelDataConverter.ITEM_TYPE, itemType)
							.set(ItemCfRecommModelDataConverter.USER_TYPE, FlinkTypeConverter.getTypeString(userType));
					}
					new ItemCfRecommModelDataConverter(userCol, userType, itemCol).save(Tuple2.of(meta, values), out);
				}
			})
			.withBroadcastSet(itemMapTable, "ITEM_MAP")
			.name("build_model");

		this.setOutput(outs, new ItemCfRecommModelDataConverter(userCol, userType, itemCol).getModelSchema());
		return this;
	}

	public class ItemSimilarityVectorGenerator extends RichMapPartitionFunction <Row, Row> {
		private static final long serialVersionUID = 4250780052412233802L;
		private String itemCol;
		private long itemNum;

		public ItemSimilarityVectorGenerator(String itemCol) {
			this.itemCol = itemCol;
		}

		@Override
		public void open(Configuration configuration) {
			List <Row> distinctNumber = getRuntimeContext().getBroadcastVariable(USER_NUM);
			for (Row row : distinctNumber) {
				if (row.getField(0).equals(itemCol)) {
					itemNum = (long) row.getField(1);
					break;
				}
			}
		}

		@Override
		public void mapPartition(Iterable <Row> iterable, Collector <Row> collector) {
			for (Row row : iterable) {
				TreeMap <Integer, Double> treeMap = new TreeMap <>();
				Object itemId = row.getField(0);
				Tuple2 <List <Object>, List <Object>> itemSimilarity = NearestNeighborsMapper.extractKObject(
					(String) row.getField(1), Long.class);
				for (int i = 0; i < itemSimilarity.f0.size(); i++) {
					long neighborId = (long) itemSimilarity.f0.get(i);
					double similarity = 1.0 - (double) itemSimilarity.f1.get(i);
					if (!itemId.equals(neighborId)) {
						treeMap.put((int) neighborId, similarity);
					}
				}
				collector.collect(Row.of(null, itemId, new SparseVector((int) itemNum, treeMap)));
			}
		}
	}

	public class UserItemVectorGenerator extends RichGroupReduceFunction <Row, Row> {
		private static final long serialVersionUID = 4250780052412233802L;
		private String rateCol;
		private String itemCol;
		private long itemNum;

		public UserItemVectorGenerator(String rateCol, String itemCol) {
			this.rateCol = rateCol;
			this.itemCol = itemCol;
		}

		@Override
		public void open(Configuration configuration) {
			List <Row> distinctNumber = getRuntimeContext().getBroadcastVariable(USER_NUM);
			for (Row row : distinctNumber) {
				if (row.getField(0).equals(itemCol)) {
					itemNum = (long) row.getField(1);
					break;
				}
			}
		}

		@Override
		public void reduce(Iterable <Row> iterable, Collector <Row> collector) {
			TreeMap <Integer, Double> treeMap = new TreeMap <>();
			Object userId = null;
			for (Row row : iterable) {
				if (null == userId) {
					userId = row.getField(0);
				}
				Preconditions.checkNotNull(row.getField(0), "User column is null!");
				long itemId = (long) row.getField(1);
				double rate = 1.0;
				if (null != rateCol) {
					rate = ((Number) row.getField(2)).doubleValue();
				}
				treeMap.put((int) itemId, rate);
			}
			collector.collect(Row.of(userId, null, new SparseVector((int) itemNum, treeMap)));
		}
	}

	public class ItemVectorGenerator extends RichGroupReduceFunction <Row, Row> {
		private static final long serialVersionUID = 1783010539701599910L;
		private String rateCol;
		private String userCol;
		private long userNum;

		public ItemVectorGenerator(String rateCol, String userCol) {
			this.rateCol = rateCol;
			this.userCol = userCol;
		}

		@Override
		public void open(Configuration configuration) {
			List <Row> distinctNumber = getRuntimeContext().getBroadcastVariable(USER_NUM);
			for (Row row : distinctNumber) {
				if (row.getField(0).equals(userCol)) {
					userNum = (long) row.getField(1);
					break;
				}
			}
		}

		@Override
		public void reduce(Iterable <Row> iterable, Collector <Row> collector) {
			TreeMap <Integer, Double> treeMap = new TreeMap <>();
			Object itemId = null;
			for (Row row : iterable) {
				if (null == itemId) {
					Preconditions.checkNotNull(row.getField(1), "Item column is null!");
					itemId = row.getField(1);
				}
				Preconditions.checkNotNull(row.getField(0), "User column is null!");
				long userId = (long) row.getField(0);
				double rate = 1.0;
				if (null != rateCol) {
					rate = ((Number) row.getField(2)).doubleValue();
				}
				treeMap.put((int) userId, rate);
			}
			collector.collect(Row.of(itemId, new SparseVector((int) userNum, treeMap)));
		}
	}

	@Override
	public ItemCfModelInfoBatchOp getModelInfoBatchOp() {
		return new ItemCfModelInfoBatchOp(this.getParams()).linkFrom(this);
	}
}
