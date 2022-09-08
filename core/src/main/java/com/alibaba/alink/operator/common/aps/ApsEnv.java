package com.alibaba.alink.operator.common.aps;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;

import static com.alibaba.alink.operator.common.aps.ApsContext.ALINK_APS_CUR_CHECK_POINT;
import static com.alibaba.alink.operator.common.aps.ApsContext.ALINK_APS_NUM_ITER;
import static com.alibaba.alink.operator.common.aps.ApsContext.alinkApsBreakAll;
import static com.alibaba.alink.operator.common.aps.ApsContext.alinkApsCurBlock;
import static com.alibaba.alink.operator.common.aps.ApsContext.alinkApsNumMiniBatch;

public class ApsEnv<DT, MT> implements Serializable {
	public static final Logger LOG = LoggerFactory.getLogger(ApsEnv.class);
	private static final int RETRY_TIMES = 10;
	private static final long CHECKPOINT_LIFE_CYCLE = 28L;
	private final static String CKPT_PREFIX = "alink_aps_tmp_ckpt";
	private static final long serialVersionUID = 8477870238598909628L;

	private final ApsSerializeData <DT> apsSerializeData;
	private final ApsSerializeModel <MT> apsSerializeModel;

	private final transient Long mlEnvId;
	private final transient ApsCheckpoint checkpoint;
	private transient boolean isBreakAll;

	@Deprecated
	public ApsEnv(ApsCheckpoint checkpoint,
				  ApsSerializeData <DT> apsSerializeData,
				  ApsSerializeModel <MT> apsSerializeModel) {
		this(
			checkpoint,
			apsSerializeData,
			apsSerializeModel,
			MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID
		);
	}

	public ApsEnv(ApsCheckpoint checkpoint,
				  ApsSerializeData <DT> apsSerializeData,
				  ApsSerializeModel <MT> apsSerializeModel,
				  Long mlEnvId) {
		this.apsSerializeData = apsSerializeData;
		this.checkpoint = checkpoint;
		this.apsSerializeModel = apsSerializeModel;
		this.mlEnvId = mlEnvId;
	}

	public DataSet <Tuple2 <Long, MT>> iterate(DataSet <Tuple2 <Long, MT>> model,
											   DataSet <DT> trainData,
											   ApsContext context,
											   boolean persistentBeforeExce,
											   final int numIter,
											   final int numCheckpoint,
											   Params params,
											   ApsIterator <DT, MT> trainIter) {
		return iterate
			(
				model,
				trainData,
				context,
				null,
				persistentBeforeExce,
				numIter,
				numCheckpoint,
				params,
				trainIter
			).f0;
	}

	public Tuple2 <DataSet <Tuple2 <Long, MT>>, BatchOperator[]> iterate(DataSet <Tuple2 <Long, MT>> model,
																		 DataSet <DT> trainData,
																		 ApsContext context,
																		 BatchOperator[] others,
																		 boolean persistentBeforeExce,
																		 final int numIter,
																		 final int numCheckpoint,
																		 Params params,
																		 ApsIterator <DT, MT> trainIter) {
		return iterate(
			model,
			trainData,
			context,
			others,
			persistentBeforeExce,
			numIter,
			numCheckpoint,
			params,
			trainIter,
			null);
	}

	public Tuple2 <DataSet <Tuple2 <Long, MT>>, BatchOperator[]> iterate(DataSet <Tuple2 <Long, MT>> model,
																		 DataSet <DT> trainData,
																		 ApsContext context,
																		 BatchOperator[] others,
																		 boolean persistentBeforeExce,
																		 final int numIter,
																		 final int numCheckpoint,
																		 Params params,
																		 ApsIterator <DT, MT> trainIter,
																		 PersistentHook modelAfterHook) {
		DataSet <Tuple2 <Long, DT>> data = DataSetUtils.zipWithIndex(trainData);

		context = context.map(new MapFunction <Params, Params>() {
			private static final long serialVersionUID = 1571322331049253987L;

			@Override
			public Params map(Params param) throws Exception {
				return param.set(ApsContext.ALINK_APS_NUM_CHECK_POINT, numCheckpoint)
					.set(ALINK_APS_NUM_ITER, numIter);
			}
		});

		if (persistentBeforeExce) {
			Tuple4 <DataSet <Tuple2 <Long, MT>>, DataSet <Tuple2 <Long, DT>>,
				ApsContext, BatchOperator[]>
				persistentAll = persistentAll(
				model, data, context,
				others, "input"
			);
			model = persistentAll.f0;
			data = persistentAll.f1;
			context = persistentAll.f2;
			others = persistentAll.f3;

			if (modelAfterHook != null) {
				model = modelAfterHook.hook(model);
			}
		}

		for (int step = 0; step < numCheckpoint; ++step) {

			LOG.info("ckptStart:{}", step);

			ApsContext curContext = context.clone().put(new Params().set(ALINK_APS_CUR_CHECK_POINT, step));

			IterativeDataSet <Tuple2 <Long, MT>> loop = model.iterate(Integer.MAX_VALUE);

			curContext.updateLoopInfo(loop);

			Tuple2 <DataSet <Tuple2 <Long, MT>>, ApsContext> res = trainIter.train(loop,
				getPartition(data, curContext), curContext, others, params);

			model = res.f0;
			curContext = res.f1;

			model = loop.closeWith(model, curContext.getCriterion());

			model = persistentModel(model, "model_" + step);

			if (modelAfterHook != null) {
				model = modelAfterHook.hook(model);
			}

			if (breakAll()) {
				break;
			}
		}

		return new Tuple2 <>(model, others);
	}

	public boolean available() {
		return null != this.checkpoint;
	}

	public boolean breakAll() {
		return !available() || isBreakAll;
	}

	public Tuple4 <DataSet <Tuple2 <Long, MT>>, DataSet <Tuple2 <Long, DT>>, ApsContext,
		BatchOperator[]> persistentAll(
		DataSet <Tuple2 <Long, MT>> model,
		DataSet <Tuple2 <Long, DT>> data,
		ApsContext context,
		BatchOperator[] others,
		String prefix) {

		if (available()) {
			TypeInformation <DT> dtType = (null == data) ?
				null : ((TupleTypeInfo <Tuple2 <Long, DT>>) data.getType()).getTypeAt(1);

			int nOthers = (null == others) ? 0 : others.length;
			BatchOperator[] inOps = new BatchOperator[nOthers + 3];
			inOps[0] = apsSerializeModel.serializeModel(model, mlEnvId);
			inOps[1] = (null == data) ? null : apsSerializeData.serializeData(data, mlEnvId);
			inOps[2] = (null == context) ? null : context.serialize(mlEnvId);

			for (int i = 0; i < nOthers; i++) {
				inOps[3 + i] = others[i];
			}

			BatchOperator[] outOps = persistentWithRetry(prefix, inOps);

			model = (null == outOps[0]) ? null : apsSerializeModel.deserilizeModel(outOps[0], model.getType());
			data = (null == outOps[1]) ? null : apsSerializeData.deserializeData(outOps[1], dtType);
			context = (null == outOps[2]) ? null : ApsContext.deserilize(outOps[2]);
			BatchOperator[] outOthers = null;
			if (nOthers > 0) {
				outOthers = new BatchOperator[nOthers];
				for (int i = 0; i < nOthers; i++) {
					outOthers[i] = outOps[3 + i];
				}
			}
			return new Tuple4 <>(model, data, context, outOthers);
		} else {
			return new Tuple4 <>(model, data, context, others);
		}
	}

	private BatchOperator[] persistentWithRetry(String prefix, BatchOperator[] inputs) {
		BatchOperator[] ret = null;

		for (int j = 0; j < RETRY_TIMES; ++j) {
			try {
				ret = persistent(CKPT_PREFIX + "_" + prefix + "_" + j, inputs);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("ckpt retry:  j: " + j);
				if (j == RETRY_TIMES - 1) {
					throw new AkUnclassifiedErrorException("Error. ",e);
				}
				continue;
			}
			break;
		}

		return ret;
	}

	private BatchOperator[] persistent(String prefix, BatchOperator[] inputs) throws Exception {
		if (available()) {
			if (inputs == null || inputs.length == 0) {
				return null;
			}

			String[] tmpSinkNames = new String[inputs.length];

			for (int i = 0; i < inputs.length; ++i) {
				if (inputs[i] == null) {
					continue;
				}

				tmpSinkNames[i] = TableUtil.getTempTableName(prefix + "_" + i);
				checkpoint.write(
					inputs[i], tmpSinkNames[i], mlEnvId, new Params().set(ApsContext.LIFECYCLE, CHECKPOINT_LIFE_CYCLE)
				);
			}

			JobExecutionResult jobExecutionResult = BatchOperator.getExecutionEnvironmentFromOps(inputs).execute();

			Map <String, Object> counters = jobExecutionResult.getAllAccumulatorResults();

			LOG.info("{}:{}", alinkApsBreakAll, counters);

			isBreakAll = counters.containsKey(alinkApsBreakAll)
				&& ((Integer) counters.get(alinkApsBreakAll)) > 0;

			BatchOperator <?>[] ret = new BatchOperator <?>[inputs.length];

			for (int i = 0; i < inputs.length; ++i) {
				if (tmpSinkNames[i] == null) {
					continue;
				}

				ret[i] = checkpoint.read(tmpSinkNames[i], mlEnvId, new Params());

				ret[i] = new TableSourceBatchOp(
					DataSetConversionUtil.toTable(
						mlEnvId,
						ret[i]
							.getDataSet()
							.partitionCustom(
								new Partitioner <Integer>() {
									private static final long serialVersionUID = -8803083408042377645L;

									@Override
									public int partition(Integer key, int numPartitions) {
										return key % numPartitions;
									}
								},
								new KeySelector <Row, Integer>() {
									private static final long serialVersionUID = 7152274198229092623L;

									@Override
									public Integer getKey(Row value) throws Exception {
										return new Random().nextInt(Integer.MAX_VALUE);
									}
								}
							),
						ret[i].getColNames(),
						ret[i].getColTypes())
				).setMLEnvironmentId(mlEnvId);
			}
			return ret;
		} else {
			return inputs;
		}
	}

	public DataSet <Tuple2 <Long, MT>> persistentModel(DataSet <Tuple2 <Long, MT>> model, String prefix) {
		return persistentAll(model, null, null, null, prefix).f0;
	}

	private DataSet <DT> getPartition(DataSet <Tuple2 <Long, DT>> data, ApsContext context) {
		return data
			.flatMap(
				new RichFlatMapFunction <Tuple2 <Long, DT>, DT>() {
					private static final long serialVersionUID = 343096718539266136L;
					int numMiniBatch;
					int curBlock;

					@Override
					public void open(Configuration parameters) throws Exception {
						LOG.info("{}:{}", Thread.currentThread().getName(), "open");
						Params params = (Params) getRuntimeContext().getBroadcastVariable("ApsContext")
							.get(0);

						numMiniBatch = params.getInteger(alinkApsNumMiniBatch);
						curBlock = params.getInteger(alinkApsCurBlock);
					}

					@Override
					public void close() throws Exception {
						LOG.info("{}:{}", Thread.currentThread().getName(), "close");
					}

					@Override
					public void flatMap(Tuple2 <Long, DT> t2, Collector <DT> collector) throws Exception {
						if (t2.f0 % this.numMiniBatch == this.curBlock) {
							collector.collect(t2.f1);
						}
					}
				}
			)
			.withBroadcastSet(context.getDataSet(), "ApsContext")
			.returns(((TupleTypeInfo <Tuple2 <Long, DT>>) data.getType()).getTypeAt(1))
			.name("SelectBlockData");
	}

	public interface PersistentHook<T> {
		default DataSet <T> hook(DataSet <T> input) {
			return input;
		}
	}
}
