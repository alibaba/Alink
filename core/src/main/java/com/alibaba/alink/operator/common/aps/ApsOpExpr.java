package com.alibaba.alink.operator.common.aps;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.comqueue.IterTaskObjKeeper;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is an experimental version of APS
 */
@SuppressWarnings("unchecked")
public class ApsOpExpr {

	private static final Logger LOG = LoggerFactory.getLogger(ApsOpExpr.class);

	public interface RequestIndexFunction<DT> extends Function, Serializable {
		List <Long> requestIndex(DT sample);
	}

	private static <DT, MT> Tuple2 <DataSet <Tuple3 <Integer, Integer, DT>>, DataSet <Tuple3 <Integer, Long, MT>>>
	pullBaseWithState(
		DataSet <DT> data,
		DataSet <Tuple2 <Long, MT>> model,
		RequestIndexFunction <DT> requestIndexFunction,
		final long modelStateHandle
	) {
		TypeInformation dtType = data.getType();
		TypeInformation mtType = ((TupleTypeInfo) model.getType()).getTypeAt(1);

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
			= data
			.flatMap(new RichFlatMapFunction <DT, Tuple3 <Integer, Integer, DT>>() {
				private static final long serialVersionUID = -3104031715299687938L;
				transient int pid;
				transient int idx;

				@Override
				public void open(Configuration parameters) throws Exception {
					pid = getRuntimeContext().getIndexOfThisSubtask();
					idx = 0;
				}

				@Override
				public void flatMap(DT value, Collector <Tuple3 <Integer, Integer, DT>> out) throws Exception {
					out.collect(new Tuple3 <>(pid, idx, value));
					idx++;
				}
			})
			.name("ApsPartitionTrainData")
			.returns(new TupleTypeInfo <>(Types.INT, Types.INT, dtType));

		DataSet <Tuple2 <Integer, Long>> t2PidFid = t3PidIdxRval
			.mapPartition(new RichMapPartitionFunction <Tuple3 <Integer, Integer, DT>, Tuple2 <Integer, Long>>() {
				private static final long serialVersionUID = -266021351887071153L;

				@Override
				public void mapPartition(Iterable <Tuple3 <Integer, Integer, DT>> values,
										 Collector <Tuple2 <Integer, Long>> out) throws Exception {
					Set <Long> allReq = new HashSet <>();
					Integer partitionId = null;
					for (Tuple3 <Integer, Integer, DT> value : values) {
						partitionId = value.f0;
						List <Long> req = requestIndexFunction.requestIndex(value.f2);
						allReq.addAll(req);
					}
					for (Long req : allReq) {
						out.collect(Tuple2.of(partitionId, req));
					}
				}
			})
			.returns(new TupleTypeInfo <>(Types.INT, Types.LONG))
			.name("ApsRequestIndex");

		DataSet <Tuple3 <Integer, Long, MT>> t3PidFidFval = t2PidFid
			.partitionCustom(new ModelPartitioner(), 1)
			.mapPartition(new RichMapPartitionFunction <Tuple2 <Integer, Long>, Tuple3 <Integer, Long, MT>>() {
				private static final long serialVersionUID = 1064875426581432405L;

				@Override
				public void open(Configuration parameters) throws Exception {
					LOG.info("{}:{}", Thread.currentThread().getName(), "open");
				}

				@Override
				public void close() throws Exception {
					LOG.info("{}:{}", Thread.currentThread().getName(), "close");
				}

				@Override
				public void mapPartition(Iterable <Tuple2 <Integer, Long>> values,
										 Collector <Tuple3 <Integer, Long, MT>> out) throws Exception {
					int taskId = getRuntimeContext().getIndexOfThisSubtask();
					Tuple2 <List <Tuple2 <Long, MT>>, Map <Long, Integer>> modelState
						= IterTaskObjKeeper.get(modelStateHandle, taskId);

					AkPreconditions.checkArgument(modelState != null, "Can't get model from state.");

					List <Tuple2 <Long, MT>> model = modelState.f0;
					Map <Long, Integer> fid2lid = modelState.f1;

					for (Tuple2 <Integer, Long> pidfid : values) {
						out.collect(Tuple3.of(pidfid.f0, pidfid.f1, model.get(fid2lid.get(pidfid.f1)).f1));
					}

				}
			})
			.name("ApsGetPartitionFeatureValue")
			.withBroadcastSet(model, "model")
			.returns(new TupleTypeInfo <>(Types.INT, Types.LONG, mtType));

		return new Tuple2 <>(t3PidIdxRval, t3PidFidFval);
	}

	private static <DT, MT> Tuple2 <DataSet <Tuple3 <Integer, Integer, DT>>, DataSet <Tuple3 <Integer, Long, MT>>>
	pullBase(
		DataSet <DT> data,
		DataSet <Tuple2 <Long, MT>> model,
		ApsFuncIndex4Pull <DT> requestIndex,
		ApsContext context4Index
	) {
		TypeInformation dtType = data.getType();
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

		DataSet <Tuple3 <Integer, Integer, DT>> t3PidIdxRval
			= data
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
			.name("ApsPartitionTrainData")
			.returns(new TupleTypeInfo(Types.INT, Types.INT, dtType));

		DataSet <Tuple2 <Integer, Long>> t2PidFid = t3PidIdxRval
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
			.returns(new TupleTypeInfo(Types.INT, Types.LONG, mtType));

		return new Tuple2 <>(t3PidIdxRval, t3PidFidFval);
	}

	public static <DT, MT, OT> DataSet <OT> pullProc(
		DataSet <DT> data,
		DataSet <Tuple2 <Long, MT>> model,
		ApsFuncIndex4Pull <DT> requestIndex,
		ApsContext context4Index,
		ApsFuncProc <DT, MT, OT> procData,
		ApsContext context4Proc
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
			= pullBase(data, model, requestIndex, context4Index);

		DataSet <Tuple3 <Integer, Integer, DT>> t3PidIdxRval = res.f0;
		DataSet <Tuple3 <Integer, Long, MT>> t3PidFidFval = res.f1;

		DataSet <OT> outputVal = null;

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

		return outputVal;
	}

	public static <DT, MT> DataSet <Tuple2 <Long, MT>> pullTrainPushWithState(
		DataSet <DT> data,
		DataSet <Tuple2 <Long, MT>> model,
		ApsContext curContext,
		RequestIndexFunction <DT> requestIndexFunction,
		ApsFuncTrain <DT, MT> trainSubset,
		final ApsFuncUpdateModel <MT> updateModel
	) {
		TypeInformation mtType = ((TupleTypeInfo) model.getType()).getTypeAt(1);

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

		final long modelStateHandle = IterTaskObjKeeper.getNewHandle();

		// initialize state
		model = model
			.partitionCustom(new ModelPartitioner(), 0)
			.mapPartition(new RichMapPartitionFunction <Tuple2 <Long, MT>, Tuple2 <Long, MT>>() {

				private static final long serialVersionUID = -4095984329300130615L;

				@Override
				public void open(Configuration parameters) throws Exception {
					Params ctx = (Params) getRuntimeContext().getBroadcastVariable("curContext").get(0);
					if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
						System.out.println("\n** " + ctx.toJson());
						LOG.info("init state:{}", Thread.currentThread().getName());
					}
				}

				@Override
				public void mapPartition(Iterable <Tuple2 <Long, MT>> values, Collector <Tuple2 <Long, MT>> out)
					throws Exception {
					int step = getIterationRuntimeContext().getSuperstepNumber();
					int taskId = getRuntimeContext().getIndexOfThisSubtask();
					if (step == 1) {
						List <Tuple2 <Long, MT>> model = new ArrayList <>();
						Map <Long, Integer> fid2lid = new HashMap <>();
						int pos = 0;
						for (Tuple2 <Long, MT> fidfval : values) {
							model.add(fidfval);
							fid2lid.put(fidfval.f0, pos);
							pos++;
						}
						if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
							System.out.println("** # feature in model " + pos);
						}
						IterTaskObjKeeper.put(modelStateHandle, taskId, Tuple2.of(model, fid2lid));
					}
				}
			})
			.name("InitState")
			.withBroadcastSet(curContext.getDataSet(), "curContext")
			.returns(new TupleTypeInfo(Types.LONG, mtType));

		Tuple2 <DataSet <Tuple3 <Integer, Integer, DT>>, DataSet <Tuple3 <Integer, Long, MT>>> res
			= pullBaseWithState(data, model, requestIndexFunction, modelStateHandle);

		DataSet <Tuple3 <Integer, Integer, DT>> t3PidIdxRval = res.f0;
		DataSet <Tuple3 <Integer, Long, MT>> t3PidFidFval = res.f1;

		DataSet <Tuple2 <Long, MT>> t2FidNewFval = t3PidFidFval
			.coGroup(t3PidIdxRval).where(0).equalTo(0)
			.sortSecondGroup(1, Order.ASCENDING)
			.withPartitioner(
				new Partitioner <Integer>() {
					private static final long serialVersionUID = 3944668070162348246L;

					@Override
					public int partition(Integer key, int numPartitions) {
						return Math.abs(key) % numPartitions;
					}
				})
			.with(trainSubset)
			.returns(new TupleTypeInfo(Types.LONG, mtType))
			.name("ApsTrainSubset");

		DataSet <Tuple2 <Long, MT>> t2FidUpdatedFval = t2FidNewFval
			.groupBy(0) // todo: use mapPartition instead
			.withPartitioner(new ModelPartitioner())
			.reduceGroup(new RichGroupReduceFunction <Tuple2 <Long, MT>, Tuple2 <Long, MT>>() {
				private static final long serialVersionUID = 9134864393612660156L;
				transient List <Tuple2 <Long, MT>> model;
				transient boolean[] collectFlag;
				transient Map <Long, Integer> fid2lid;
				transient boolean isLastStep;
				transient Collector <Tuple2 <Long, MT>> collector;

				@Override
				public void open(Configuration parameters) throws Exception {
					LOG.info("{}:{}", Thread.currentThread().getName(), "open");
					isLastStep = getRuntimeContext().getBroadcastVariable("stopCriterion").isEmpty();
					int taskId = getRuntimeContext().getIndexOfThisSubtask();
					Tuple2 <List <Tuple2 <Long, MT>>, Map <Long, Integer>> modelState;
					if (isLastStep) {
						modelState = IterTaskObjKeeper.remove(modelStateHandle, taskId);
						collectFlag = new boolean[modelState.f0.size()];
						Arrays.fill(collectFlag, false);
					} else {
						modelState = IterTaskObjKeeper.get(modelStateHandle, taskId);
						if (modelState == null) {
							throw new AkUnclassifiedErrorException("Fail to get model for task " + taskId);
						}
					}
					model = modelState.f0;
					fid2lid = modelState.f1;
				}

				@Override
				public void close() throws Exception {
					if (isLastStep) {
						if (collector == null) {
							return; // FIXME
						}
						for (int i = 0; i < collectFlag.length; i++) {
							if (!collectFlag[i]) {
								Tuple2 <Long, MT> featureItem = model.get(i);
								collector.collect(featureItem);
							}
						}
					}
					LOG.info("{}:{}", Thread.currentThread().getName(), "close");
				}

				@Override
				public void reduce(Iterable <Tuple2 <Long, MT>> values, Collector <Tuple2 <Long, MT>> out)
					throws Exception {
					List <MT> newFeaVals = new ArrayList <>();
					Long fid = null;
					for (Tuple2 <Long, MT> fidfval : values) {
						newFeaVals.add(fidfval.f1);
						fid = fidfval.f0;
					}
					if (fid == null) {
						return;
					}
					int pos = fid2lid.get(fid);
					MT oldFeaValue = model.get(pos).f1;
					MT updatedFeaValue = updateModel.update(oldFeaValue, newFeaVals);
					if (isLastStep) {
						out.collect(Tuple2.of(fid, updatedFeaValue));
						collector = out;
						collectFlag[pos] = true;
					} else {
						Tuple2 <Long, MT> feature = model.get(pos);
						feature.f1 = updatedFeaValue;
					}
				}
			})
			.name("ApsUpdateModel")
			.withBroadcastSet(curContext.getCriterion(), "stopCriterion")
			.returns(new TupleTypeInfo <>(Types.LONG, mtType));

		return t2FidUpdatedFval;
	}

	private static class ModelPartitioner implements Partitioner <Long> {
		private static final long serialVersionUID = -6766513370607697864L;

		@Override
		public int partition(Long key, int numPartitions) {
			return (int) (Math.abs(key) % numPartitions);
		}
	}
}
