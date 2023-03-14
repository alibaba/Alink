package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.ReservedColsWithFirstInputSpec;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.sql.builtin.agg.BaseRankUdaf;
import com.alibaba.alink.common.sql.builtin.agg.BaseUdaf;
import com.alibaba.alink.common.sql.builtin.agg.CountUdaf;
import com.alibaba.alink.common.sql.builtin.agg.LastDistinctValueUdaf;
import com.alibaba.alink.common.sql.builtin.agg.LastTimeUdaf;
import com.alibaba.alink.common.sql.builtin.agg.LastValueUdaf;
import com.alibaba.alink.common.sql.builtin.agg.ListAggUdaf;
import com.alibaba.alink.common.sql.builtin.agg.MTableAgg;
import com.alibaba.alink.common.sql.builtin.agg.SumLastUdaf;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.feature.featurebuilder.FeatureClause;
import com.alibaba.alink.operator.common.feature.featurebuilder.FeatureClauseOperator;
import com.alibaba.alink.operator.common.feature.featurebuilder.FeatureClauseUtil;
import com.alibaba.alink.operator.common.slidingwindow.SessionSharedData;
import com.alibaba.alink.params.feature.featuregenerator.OverWindowParams;

import static com.alibaba.alink.operator.common.feature.featurebuilder.WindowResColType.RES_TYPE;

/**
 * Batch over window feature builder.
 */
@NameCn("特征构造：OverWindow")
@NameEn("Over Window Feature Builder")
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@ParamSelectColumnSpec(name = "partitionCols")
@ReservedColsWithFirstInputSpec
public class OverWindowBatchOp extends BatchOperator <OverWindowBatchOp>
	implements OverWindowParams <OverWindowBatchOp> {

	public OverWindowBatchOp() {
		super(null);
	}

	public OverWindowBatchOp(Params params) {
		super(params);
	}

	@Override
	public OverWindowBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String[] inputColNames = in.getColNames();
		TypeInformation <?>[] inputColTypes = in.getColTypes();
		String[] partitionBys = getGroupCols();
		int[] partitionByIndices;
		if (partitionBys == null) {
			partitionByIndices = null;
		} else {
			partitionByIndices = TableUtil.findColIndices(inputColNames, partitionBys);
		}
		String orderBy = getOrderBy();
		Tuple2 <int[], Order[]> orderInfo = parseOrder(orderBy, inputColNames);
		String[] reversedCols = getReservedCols();
		if (reversedCols == null) {
			reversedCols = inputColNames;
		}
		int[] reversedIndices = TableUtil.findColIndices(inputColNames, reversedCols);

		String sqlClause = getClause();
		FeatureClause[] featureClauses = FeatureClauseUtil.extractFeatureClauses(sqlClause, in.getSchema(),
			orderBy);

		DataSet <Row> res;
		if (partitionBys != null) {
			int[] partitionIndices = TableUtil.findColIndices(inputColNames, partitionBys);
			SortedGrouping <Row> sortedGrouping = in
				.getDataSet()
				.groupBy(partitionIndices)
				.sortGroup(orderInfo.f0[0], orderInfo.f1[0]);
			for (int i = 1; i < orderInfo.f0.length; i++) {
				sortedGrouping = sortedGrouping
					.sortGroup(orderInfo.f0[i], orderInfo.f1[i]);
			}
			res = sortedGrouping.reduceGroup(
				new GroupOperation(featureClauses, orderInfo.f0, partitionByIndices, reversedIndices, inputColNames)
			);
		} else {
			DataSet <Row> inWithGroup = in
				.getDataSet()
				.mapPartition(
					new MapPartitionFunction <Row, Row>() {
						@Override
						public void mapPartition(Iterable <Row> values, Collector <Row> out) throws Exception {
							Row res = null;
							for (Row value : values) {
								int index = value.getArity();
								if (res == null) {
									res = new Row(index + 1);
								}
								for (int i = 0; i < index; i++) {
									res.setField(i, value.getField(i));
								}
								res.setField(index, 0);
								out.collect(res);
							}
						}
					}
				);

			String[] newNames = new String[inputColNames.length + 1];
			TypeInformation <?>[] newTypes = new TypeInformation[inputColTypes.length + 1];
			System.arraycopy(inputColNames, 0, newNames, 0, inputColNames.length);
			System.arraycopy(inputColTypes, 0, newTypes, 0, inputColTypes.length);
			String tempGroupCol = inputColNames[0];
			//build one no use group col.
			do {
				tempGroupCol = tempGroupCol + "1";
			} while (TableUtil.findColIndex(inputColNames, tempGroupCol) != -1);

			newNames[inputColNames.length] = tempGroupCol;
			newTypes[inputColTypes.length] = Types.INT;
			BatchOperator <?> ins = new TableSourceBatchOp(DataSetConversionUtil
				.toTable(getMLEnvironmentId(), inWithGroup, newNames, newTypes));
			SortedGrouping <Row> sortedPartition = ins
				.getDataSet()
				.groupBy(inputColNames.length)
				.sortGroup(orderInfo.f0[0], orderInfo.f1[0]);
			for (int i = 1; i < orderInfo.f0.length; i++) {
				sortedPartition = sortedPartition
					.sortGroup(orderInfo.f0[i], orderInfo.f1[i]);
			}
			res = sortedPartition
				.reduceGroup(new GroupOperation(featureClauses, orderInfo.f0,
					partitionByIndices, reversedIndices, inputColNames));
		}

		String[] resColNames = new String[featureClauses.length + reversedCols.length];
		TypeInformation <?>[] resColTypes = new TypeInformation[featureClauses.length + reversedCols.length];
		for (int i = 0; i < featureClauses.length; i++) {
			int featureIndex = i + reversedCols.length;
			resColNames[featureIndex] = featureClauses[i].outColName;
			if (RES_TYPE.equals(featureClauses[i].op.getResType())) {
				if (featureClauses[i].op.equals(FeatureClauseOperator.LAST_DISTINCT)) {
					resColTypes[featureIndex] = inputColTypes[TableUtil.findColIndex(inputColNames,
						(String) featureClauses[i].inputParams[0])];
				} else {
					resColTypes[featureIndex] = inputColTypes[TableUtil.findColIndex(inputColNames,
						featureClauses[i].inColName)];
				}
			} else {
				resColTypes[featureIndex] = featureClauses[i].op.getResType();
			}
		}
		for (int i = 0; i < reversedCols.length; i++) {
			resColNames[i] = reversedCols[i];
			resColTypes[i] = inputColTypes[TableUtil.findColIndex(inputColNames,
				reversedCols[i])];
		}

		this.setOutput(res, resColNames, resColTypes);
		return this;
	}

	private Tuple2 <int[], Order[]> parseOrder(String orderClause, String[] inputColNames) {
		String[] orders = orderClause.split(",");
		int[] orderIndices = new int[orders.length];
		Order[] orderTypes = new Order[orders.length];
		for (int i = 0; i < orders.length; i++) {
			String[] localOrder = orders[i].trim().split(" ");
			orderIndices[i] = TableUtil.findColIndex(inputColNames, localOrder[0]);
			if (localOrder.length == 1) {
				orderTypes[i] = Order.ASCENDING; //default is descending
			} else {
				String stringOrder = localOrder[1].trim().toLowerCase();
				if ("desc".equals(stringOrder) || "descending".equals(stringOrder)) {
					orderTypes[i] = Order.DESCENDING;
				} else if ("asc".equals(stringOrder) || "ascending".equals(stringOrder)) {
					orderTypes[i] = Order.ASCENDING;
				} else {
					throw new AkIllegalOperatorParameterException(
						String.format("order [%s] not support yet.", stringOrder));
				}
			}
		}
		return Tuple2.of(orderIndices, orderTypes);
	}

	private static class GroupOperation extends RichGroupReduceFunction <Row, Row> {
		FeatureClause[] featureClauses;
		int sessionId;
		int[] partitionByIndices;
		int[] reversedIndices;
		String[] inputColNames;
		int[] orderIndices;

		GroupOperation(FeatureClause[] featureClauses, int[] orderIndices,
					   int[] partitionByIndices, int[] reversedIndices, String[] inputColNames) {
			this.featureClauses = featureClauses;
			this.orderIndices = orderIndices;
			this.partitionByIndices = partitionByIndices;
			this.reversedIndices = reversedIndices;
			this.inputColNames = inputColNames;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			//new open func in worker, so sessionId new here, not constructor.
			this.sessionId = SessionSharedData.getNewSessionId();
		}

		@Override
		public void reduce(Iterable <Row> values, Collector <Row> out) throws Exception {
			Row res = null;
			StringBuilder keys = new StringBuilder();
			boolean noGroupCol = partitionByIndices == null || partitionByIndices.length == 0;
			if (noGroupCol) {
				keys.append("i");
			}

			Object[] thisData = null;
			for (Row value : values) {
				if (res == null) {
					res = new Row(reversedIndices.length + featureClauses.length);
				}
				if (!noGroupCol) {
					keys.setLength(0);
					for (int index : partitionByIndices) {
						keys.append(value.getField(index).toString()).append("_");
					}
				}

				BaseUdaf <?, ?>[] calcs = (BaseUdaf <?, ?>[]) SessionSharedData.get(keys.toString(), sessionId);
				if (calcs == null) {
					calcs = new BaseUdaf[featureClauses.length];
					for (int i = 0; i < featureClauses.length; i++) {
						calcs[i] = featureClauses[i].op.getCalc();
					}
				}

				Long index = (Long) SessionSharedData.get(keys + "_index", sessionId);
				if (index == null) {
					index = 0L;
				} else {
					index++;
				}

				for (int i = 0; i < featureClauses.length; i++) {

					/*
					  rank, dense_rank and row_time do not need additional selected cols.
					  count has 'count(*)' and 'count(1)' usage.

					  Beside, 'distinct' and 'all' are not supported.
					 */
					BaseUdaf <?, ?> udaf = calcs[i];
					if (udaf instanceof LastValueUdaf || udaf instanceof LastDistinctValueUdaf ||
						udaf instanceof LastTimeUdaf || udaf instanceof SumLastUdaf) {
						int kLength = featureClauses[i].inputParams.length;
						Object[] aggInputData;
						int inputIndex = 0;

						aggInputData = new Object[kLength + 3];
						aggInputData[inputIndex++] = value.getField(
							TableUtil.findColIndex(inputColNames, featureClauses[i].inColName));

						if (udaf instanceof LastDistinctValueUdaf) {
							for (int j = 0; j < kLength; j++) {
								aggInputData[inputIndex++] = value.getField(
									TableUtil.findColIndex(inputColNames, (String) featureClauses[i].inputParams[0]));
							}
						} else {
							for (int j = 0; j < kLength; j++) {
								// LastValueUdaf, LastTimeUdaf, SumLastUdaf need input param as the other input.
								aggInputData[inputIndex++] = featureClauses[i].inputParams[j];
							}
						}

						aggInputData[inputIndex++] = index;
						aggInputData[inputIndex] = -1;
						if (udaf instanceof LastValueUdaf && aggInputData[1] instanceof String) {
							aggInputData[1] = Integer.parseInt((String) aggInputData[1]);
						}
						udaf.accumulateBatch(aggInputData);
						SessionSharedData.put(keys + "_index", sessionId, index);
					} else if (udaf instanceof BaseRankUdaf) {
						if (thisData == null) {
							thisData = new Object[orderIndices.length];
						}
						for (int j = 0; j < orderIndices.length; j++) {
							thisData[j] = value.getField(orderIndices[j]);
						}
						udaf.accumulateBatch(thisData);
					} else if (udaf instanceof CountUdaf) {
						udaf.accumulateBatch(0);
					} else if (udaf instanceof MTableAgg) {
						Object[] aggInputData = new Object[featureClauses[i].inputParams.length + 1];
						aggInputData[0] = value.getField(
							TableUtil.findColIndex(inputColNames, featureClauses[i].inColName));
						for (int j = 0; j < featureClauses[i].inputParams.length; j++) {
							aggInputData[1 + j] = value.getField(
								TableUtil.findColIndex(inputColNames, (String)featureClauses[i].inputParams[j]));
						}

						udaf.accumulateBatch(aggInputData);
					} else {
						Object[] aggInputData = new Object[featureClauses[i].inputParams.length + 1];
						aggInputData[0] = value.getField(
							TableUtil.findColIndex(inputColNames, featureClauses[i].inColName));
						System.arraycopy(featureClauses[i].inputParams, 0, aggInputData, 1,
							featureClauses[i].inputParams.length);
						//currently listAgg fills delimiter, lastDistinct fills col name, others fill int data.
						if (!(udaf instanceof ListAggUdaf)) {
							for (int i1 = 1; i1 < aggInputData.length; i1++) {
								aggInputData[i1] = Integer.parseInt((String) aggInputData[i1]);
							}
						}
						udaf.accumulateBatch(aggInputData);
					}
					SessionSharedData.put(keys.toString(), sessionId, calcs);
					res.setField(i + reversedIndices.length, udaf.getValueBatch());
				}

				for (int i = 0; i < reversedIndices.length; i++) {
					res.setField(i, value.getField(reversedIndices[i]));
				}

				out.collect(res);
			}
		}

	}
}
