package com.alibaba.alink.operator.common.aps;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.operator.batch.BatchOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

@SuppressWarnings("unchecked")
public class ApsOp {

	private static final Logger LOG = LoggerFactory.getLogger(ApsOp.class);

	private static <DT, MT> Tuple2 <DataSet <Tuple3 <Integer, Integer, DT>>, DataSet <Tuple3 <Integer, Long, MT>>>
	pullBase(
		DataSet <DT> data,
		DataSet <Tuple2 <Long, MT>> model,
		ApsFuncIndex4Pull <DT> requestIndex,
		ApsContext context4Index,
		Integer trainNum
	) {
		TypeInformation dtType = data.getType();
		TypeInformation mtType;

		if (model.getType().isTupleType() && model.getType().getArity() == 2) {
			mtType = ((TupleTypeInfo) model.getType()).getTypeAt(1);
		} else {
			throw new AkUnclassifiedErrorException("Unsupported model type. type: " + model.getType().toString());
		}

		int nPartition = BatchOperator
			.getExecutionEnvironmentFromDataSets(data, model)
			.getParallelism();
		if (null != trainNum && trainNum > 0) {
			nPartition = trainNum;
		}

		/**
		 *
		 * The data consists of records, and the model consists of features.
		 * Rval : record value
		 * Fid  : feature id
		 * Fval : feature value
		 *
		 * Data is divided into several partitions for processing
		 * Pid  : partition id
		 * Idx  : temp index for the records in each partition
		 *
		 */

		DataSet <Tuple3 <Integer, Integer, DT>> t3PidIdxRval
			= data.rebalance()
			.mapPartition(
				new RichMapPartitionFunction <DT, Tuple3 <Integer, Integer, DT>>() {
					private static final long serialVersionUID = 4694589881648017911L;

					@Override
					public void mapPartition(Iterable <DT> values,
											 Collector <Tuple3 <Integer, Integer, DT>> out)
						throws Exception {
						int pid = getRuntimeContext().getIndexOfThisSubtask();
						int idx = 0;
						for (DT t : values) {
							out.collect(new Tuple3 <>(pid, idx, t));
							idx++;
						}
					}
				})
			.setParallelism(nPartition)
			.name("ApsPartitionTrainData")
			.returns(new TupleTypeInfo(Types.INT, Types.INT, dtType));

		DataSet <Tuple2 <Integer, Long>> t2PidFid = null;
		if (null == context4Index) {
			t2PidFid = t3PidIdxRval
				.groupBy(0)
				.withPartitioner(new Partitioner <Integer>() {
					private static final long serialVersionUID = 3346879831879931555L;

					@Override
					public int partition(Integer key, int numPartitions) {
						return Math.abs(key) % numPartitions;
					}
				})
				.sortGroup(1, Order.ASCENDING)
				.reduceGroup(requestIndex)
				.name("ApsRequestIndex");
		} else {
			t2PidFid = t3PidIdxRval
				.groupBy(0)
				.withPartitioner(new Partitioner <Integer>() {
					private static final long serialVersionUID = 5245425039908176380L;

					@Override
					public int partition(Integer key, int numPartitions) {
						return Math.abs(key) % numPartitions;
					}
				})
				.sortGroup(1, Order.ASCENDING)
				.reduceGroup(requestIndex)
				.withBroadcastSet(context4Index.getDataSet(), "RequestIndex")
				.name("ApsRequestIndex");
		}

		DataSet <Tuple3 <Integer, Long, MT>> t3PidFidFval
			= t2PidFid
			.coGroup(model).where(1).equalTo(0)
			.with(
				new RichCoGroupFunction <Tuple2 <Integer, Long>, Tuple2 <Long, MT>, Tuple3 <Integer, Long, MT>>() {
					private static final long serialVersionUID = 7387404099507374176L;

					@Override
					public void open(Configuration parameters) throws Exception {
						LOG.info("{}:{}", Thread.currentThread().getName(), "open");
					}

					@Override
					public void close() throws Exception {
						LOG.info("{}:{}", Thread.currentThread().getName(), "close");
					}

					@Override
					public void coGroup(Iterable <Tuple2 <Integer, Long>> first,
										Iterable <Tuple2 <Long, MT>> second,
										Collector <Tuple3 <Integer, Long, MT>> out) throws Exception {

						ArrayList <Tuple2 <Integer, Long>> firstList = new ArrayList <>();

						for (Tuple2 <Integer, Long> pidfid : first) {
							firstList.add(pidfid);
						}

						if (!firstList.isEmpty()) {
							for (Tuple2 <Long, MT> fidfval : second) {
								for (Tuple2 <Integer, Long> pidfid : firstList) {
									out.collect(
										new Tuple3 <>(pidfid.f0, pidfid.f1, fidfval.f1));
								}
							}
						}
					}
				})
			.name("ApsGetPartitionFeatureValue")
			.returns(new TupleTypeInfo <>(Types.INT, Types.LONG, mtType));

		return new Tuple2 <>(t3PidIdxRval, t3PidFidFval);
	}

	public static <DT, MT, OT> DataSet <OT> pullProc(
		DataSet <DT> data,
		DataSet <Tuple2 <Long, MT>> model,
		ApsFuncIndex4Pull <DT> requestIndex,
		ApsFuncProc <DT, MT, OT> procData
	) {
		return pullProc(
			data,
			model,
			requestIndex,
			null,
			procData,
			null);
	}

	public static <DT, MT, OT> DataSet <OT> pullProc(
		DataSet <DT> data,
		DataSet <Tuple2 <Long, MT>> model,
		ApsFuncIndex4Pull <DT> requestIndex,
		ApsContext context4Index,
		ApsFuncProc <DT, MT, OT> procData,
		ApsContext context4Proc
	) {
		return pullProc(
			data,
			model,
			requestIndex,
			context4Index,
			procData,
			context4Proc,
			null);
	}

	public static <DT, MT, OT> DataSet <OT> pullProc(
		DataSet <DT> data,
		DataSet <Tuple2 <Long, MT>> model,
		ApsFuncIndex4Pull <DT> requestIndex,
		ApsContext context4Index,
		ApsFuncProc <DT, MT, OT> procData,
		ApsContext context4Proc,
		Integer trainNum
	) {
		/**
		 *
		 * The data consists of records, and the model consists of features.
		 * Rval : record value
		 * Fid  : feature id
		 * Fval : feature value
		 *
		 * Data is divided into several partitions for processing
		 * Pid  : partition id
		 * Idx  : temp index for the records in each partition
		 *
		 */

		Tuple2 <DataSet <Tuple3 <Integer, Integer, DT>>, DataSet <Tuple3 <Integer, Long, MT>>> res
			= pullBase(data, model, requestIndex, context4Index, trainNum);

		DataSet <Tuple3 <Integer, Integer, DT>> t3PidIdxRval = res.f0;
		DataSet <Tuple3 <Integer, Long, MT>> t3PidFidFval = res.f1;

		DataSet <OT> outputVal = null;
		if (null == context4Proc) {
			outputVal = t3PidFidFval
				.coGroup(t3PidIdxRval).where(0).equalTo(0)
				.sortSecondGroup(1, Order.ASCENDING)
				.withPartitioner(
					new Partitioner <Integer>() {
						private static final long serialVersionUID = -2886394771121082255L;

						@Override
						public int partition(Integer key, int numPartitions) {
							return Math.abs(key) % numPartitions;
						}
					})
				.with(procData)
				.name("ApsTrainSubset");
		} else {
			outputVal = t3PidFidFval
				.coGroup(t3PidIdxRval).where(0).equalTo(0)
				.sortSecondGroup(1, Order.ASCENDING)
				.withPartitioner(
					new Partitioner <Integer>() {
						private static final long serialVersionUID = 2862199187133765866L;

						@Override
						public int partition(Integer key, int numPartitions) {
							return Math.abs(key) % numPartitions;
						}
					})
				.with(procData)
				.withBroadcastSet(context4Proc.getDataSet(), "TrainSubset")
				.name("ApsTrainSubset");
		}

		return outputVal;
	}

	public static <DT, MT> DataSet <Tuple2 <Long, MT>> pullTrainPush(
		DataSet <DT> data,
		DataSet <Tuple2 <Long, MT>> model,
		ApsFuncIndex4Pull <DT> requestIndex,
		ApsFuncTrain <DT, MT> trainSubset,
		ApsFuncUpdateModel <MT> updateModel
	) {
		return pullTrainPush(
			data,
			model,
			requestIndex,
			null,
			trainSubset,
			null,
			updateModel
		);
	}

	public static <DT, MT> DataSet <Tuple2 <Long, MT>> pullTrainPush(
		DataSet <DT> data,
		DataSet <Tuple2 <Long, MT>> model,
		ApsFuncIndex4Pull <DT> requestIndex,
		ApsContext context4Index,
		ApsFuncTrain <DT, MT> trainSubset,
		ApsContext context4Train,
		ApsFuncUpdateModel <MT> updateModel
	) {
		return pullTrainPush(
			data,
			model,
			requestIndex,
			context4Index,
			trainSubset,
			context4Train,
			updateModel,
			null
		);
	}

	public static <DT, MT> DataSet <Tuple2 <Long, MT>> pullTrainPush(
		DataSet <DT> data,
		DataSet <Tuple2 <Long, MT>> model,
		ApsFuncIndex4Pull <DT> requestIndex,
		ApsContext context4Index,
		ApsFuncTrain <DT, MT> trainSubset,
		ApsContext context4Train,
		ApsFuncUpdateModel <MT> updateModel,
		Integer trainNum
	) {
		TypeInformation mtType;

		if (model.getType().isTupleType() && model.getType().getArity() == 2) {
			mtType = ((TupleTypeInfo) model.getType()).getTypeAt(1);
		} else {
			throw new AkUnclassifiedErrorException("Unsupported model type. type: " + model.getType().toString());
		}

		/**
		 *
		 * The data consists of records, and the model consists of features.
		 * Rval : record value
		 * Fid  : feature id
		 * Fval : feature value
		 *
		 * Data is divided into several partitions for processing
		 * Pid  : partition id
		 * Idx  : temp index for the records in each partition
		 *
		 */

		Tuple2 <DataSet <Tuple3 <Integer, Integer, DT>>, DataSet <Tuple3 <Integer, Long, MT>>> res
			= pullBase(data, model, requestIndex, context4Index, trainNum);

		DataSet <Tuple3 <Integer, Integer, DT>> t3PidIdxRval = res.f0;
		DataSet <Tuple3 <Integer, Long, MT>> t3PidFidFval = res.f1;

		DataSet <Tuple2 <Long, MT>> t2FidNewFval = null;
		if (null == context4Train) {
			t2FidNewFval = t3PidFidFval
				.coGroup(t3PidIdxRval).where(0).equalTo(0)
				.sortSecondGroup(1, Order.ASCENDING)
				.withPartitioner(
					new Partitioner <Integer>() {
						private static final long serialVersionUID = -3770586162489589601L;

						@Override
						public int partition(Integer key, int numPartitions) {
							return Math.abs(key) % numPartitions;
						}
					})
				.with(trainSubset)
				.name("ApsTrainSubset");
		} else {
			t2FidNewFval = t3PidFidFval
				.coGroup(t3PidIdxRval).where(0).equalTo(0)
				.sortSecondGroup(1, Order.ASCENDING)
				.withPartitioner(
					new Partitioner <Integer>() {
						private static final long serialVersionUID = 2234202213103891149L;

						@Override
						public int partition(Integer key, int numPartitions) {
							return Math.abs(key) % numPartitions;
						}
					})
				.with(trainSubset)
				.withBroadcastSet(context4Train.getDataSet(), "TrainSubset")
				.name("ApsTrainSubset");
		}

		t2FidNewFval = ((CoGroupOperator) t2FidNewFval).returns(new TupleTypeInfo(Types.LONG, mtType));

		DataSet <Tuple2 <Long, MT>> t2FidUpdatedFval
			= t2FidNewFval
			.coGroup(model).where(0).equalTo(0)
			.with(updateModel)
			.name("ApsUpdateModel")
			.returns(new TupleTypeInfo(Types.LONG, mtType));

		return t2FidUpdatedFval;
	}

	public static <MT> DataSet <Tuple2 <Long, MT>> push(
		DataSet <Tuple2 <Long, MT>> upadatedFeatures,
		DataSet <Tuple2 <Long, MT>> model,
		ApsFuncUpdateModel <MT> updateModel
	) {
		TypeInformation mtType;
		if (model.getType().isTupleType() && model.getType().getArity() == 2) {
			mtType = ((TupleTypeInfo) model.getType()).getTypeAt(1);
		} else {
			throw new AkUnclassifiedErrorException("Unsupported model type. type: " + model.getType().toString());
		}

		/**
		 *
		 * The model consists of features.
		 * Fid  : feature id
		 * Fval : feature value
		 *
		 */

		DataSet <Tuple2 <Long, MT>> t2FidUpdatedFval
			= upadatedFeatures
			.coGroup(model).where(0).equalTo(0)
			.with(updateModel)
			.name("ApsUpdateModel")
			.returns(new TupleTypeInfo(Types.LONG, mtType));

		return t2FidUpdatedFval;
	}

	private static class ModelPartitioner implements Partitioner <Long> {
		private static final long serialVersionUID = -1091580614455494830L;

		@Override
		public int partition(Long key, int numPartitions) {
			return (int) (Math.abs(key) % numPartitions);
		}
	}
}
