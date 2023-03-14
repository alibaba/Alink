package com.alibaba.alink.operator.common.aps;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.io.directreader.DefaultDistributedInfo;
import com.alibaba.alink.common.io.directreader.DistributedInfo;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.NumSeqSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ApsContext {

	public static final Logger LOG = LoggerFactory.getLogger(ApsContext.class);

	public static final String alinkApsNumCheckpoint = "alinkApsNumCheckpoint";
	public static final String alinkApsNumIter = "alinkApsNumIter";
	public static final String alinkApsNumMiniBatch = "alinkApsNumMiniBatch";
	public static final String alinkApsCurCheckpoint = "alinkApsCurCheckpoint";
	public static final String alinkApsBreakAll = "alinkApsBreakAll";
	static final String alinkApsCurBlock = "alinkApsCurBlock";
	private static final String alinkApsHasNextBlock = "alinkApsHasNextBlock";
	private static final String alinkApsStepNum = "alinkApsStepNum";
	public static ParamInfo <Boolean> ALINK_APS_BREAK_ALL = ParamInfoFactory
		.createParamInfo("alinkApsBreakAll", Boolean.class)
		.setDescription("alinkApsBreakAll")
		.setRequired()
		.build();
	public static ParamInfo <Integer> ALINK_APS_NUM_MINI_BATCH = ParamInfoFactory
		.createParamInfo("alinkApsNumMiniBatch", Integer.class)
		.setDescription("alinkApsNumMiniBatch")
		.setRequired()
		.build();
	public static ParamInfo <Integer> ALINK_APS_NUM_CHECK_POINT = ParamInfoFactory
		.createParamInfo("alinkApsNumCheckpoint", Integer.class)
		.setDescription("alinkApsNumCheckpoint")
		.setRequired()
		.build();
	public static ParamInfo <Integer> ALINK_APS_NUM_ITER = ParamInfoFactory
		.createParamInfo("alinkApsNumIter", Integer.class)
		.setDescription("alinkApsNumIter")
		.setRequired()
		.build();
	public static ParamInfo <Long[]> SEEDS = ParamInfoFactory
		.createParamInfo("seeds", Long[].class)
		.setDescription("seeds")
		.setRequired()
		.build();
	public static ParamInfo <Boolean> ALINK_APS_HAS_NEXT_BLOCK = ParamInfoFactory
		.createParamInfo("alinkApsHasNextBlock", Boolean.class)
		.setDescription("alinkApsHasNextBlock")
		.setRequired()
		.build();
	public static ParamInfo <Integer> ALINK_APS_CUR_BLOCK = ParamInfoFactory
		.createParamInfo("alinkApsCurBlock", Integer.class)
		.setDescription("alinkApsCurBlock")
		.setRequired()
		.build();
	public static ParamInfo <Integer> ALINK_APS_CUR_CHECK_POINT = ParamInfoFactory
		.createParamInfo("alinkApsCurCheckpoint", Integer.class)
		.setDescription("alinkApsCurCheckpoint")
		.setRequired()
		.build();
	public static ParamInfo <Long> LIFECYCLE = ParamInfoFactory
		.createParamInfo("lifecycle", Long.class)
		.setDescription("lifecycle")
		.setRequired()
		.build();
	protected DataSet <Params> context;

	@Deprecated
	public ApsContext() {
		this(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID);
	}

	public ApsContext(long mlEnvId) {
		this.context =
			new NumSeqSourceBatchOp()
				.setFrom(1)
				.setTo(1)
				.setMLEnvironmentId(mlEnvId)
				.getDataSet()
				.map(new MapFunction <Row, Params>() {
					private static final long serialVersionUID = -1788057684839131006L;

					@Override
					public Params map(Row row) throws Exception {
						return new Params();
					}
				});
	}

	public ApsContext(DataSet <Params> info) {
		AkPreconditions.checkNotNull(info);
		this.context = info
			.map(new MapFunction <Params, Params>() {
				private static final long serialVersionUID = 3148182956495365060L;

				@Override
				public Params map(Params params) throws Exception {
					return params.clone();
				}
			});
	}

	private static DataSet <Row> seriContext(
		DataSet <Params> context) {

		return context.flatMap(new FlatMapFunction <Params, Row>() {
			private static final long serialVersionUID = 426050062920251145L;

			@Override
			public void flatMap(Params parameter, Collector <Row> collector) throws Exception {
				List <Row> seriRows = new ArrayList <>();
				appendStringToModel(parameter.toJson(), seriRows);

				for (Row row : seriRows) {
					collector.collect(row);
				}
			}
		});
	}

	public static ApsContext deserilize(BatchOperator <?> all) {
		return new ApsContext(
			all.getDataSet().reduceGroup(new GroupReduceFunction <Row, Params>() {
				private static final long serialVersionUID = 9192617145467685004L;

				@Override
				public void reduce(Iterable <Row> iterable, Collector <Params> collector) throws Exception {
					List <Row> all = new ArrayList <>();
					for (Row row : iterable) {
						all.add(row);
					}
					collector.collect(Params.fromJson(extractStringFromModel(all)));
				}
			})
		);
	}

	@Override
	public ApsContext clone() {
		return new ApsContext(this.context);
	}

	public ApsContext map(MapFunction <Params, Params> func) {
		this.context = this.context.map(func);
		return this;
	}

	public DataSet <Params> getDataSet() {
		return this.context;
	}

	public ApsContext put(Params params) {
		this.context = this.context.map(new AppendParam2Context(params));
		return this;
	}

	public ApsContext put(DataSet <Params> info) {
		this.context = this.context.map(new RichMapFunction <Params, Params>() {
			private static final long serialVersionUID = -1634936932541926298L;
			Params info;

			@Override
			public void open(Configuration parameters) throws Exception {
				info = (Params) getRuntimeContext().getBroadcastVariable("info").get(0);
			}

			@Override
			public Params map(Params value) throws Exception {
				return value.merge(info);
			}
		}).withBroadcastSet(info, "info");

		return this;
	}

	public ApsContext put(ApsContext info) {
		return put(info.context);
	}

	public <T> ApsContext put(final String key, DataSet <T> dataSet) {
		this.context = this.context.map(new RichMapFunction <Params, Params>() {
			private static final long serialVersionUID = 1639194499741682391L;
			T data;

			@Override
			public void open(Configuration parameters) throws Exception {
				this.data = (T) getRuntimeContext().getBroadcastVariable("dataSet").get(0);
			}

			@Override
			public Params map(Params value) throws Exception {
				return value.set(key, this.data);
			}
		}).withBroadcastSet(dataSet, "dataSet");

		return this;
	}

	@Deprecated
	public BatchOperator serialize() {
		return serialize(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID);
	}

	public BatchOperator <?> serialize(Long mlEnvId) {
		return new TableSourceBatchOp(
			DataSetConversionUtil.toTable(
				mlEnvId,
				seriContext(context),
				DEFAULT_MODEL_SCHEMA.getFieldNames(),
				DEFAULT_MODEL_SCHEMA.getFieldTypes())
		).setMLEnvironmentId(mlEnvId);
	}

	public <MT> ApsContext updateLoopInfo(IterativeDataSet <Tuple2 <Long, MT>> loop) {
		this.put(alinkApsStepNum,
			loop.mapPartition(
				new RichMapPartitionFunction <Tuple2 <Long, MT>, Integer>() {
					private static final long serialVersionUID = 4816930852791283240L;

					@Override
					public void mapPartition(Iterable <Tuple2 <Long, MT>> iterable, Collector <Integer> collector)
						throws Exception {
						if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
							collector.collect(getIterationRuntimeContext().getSuperstepNumber());
						}
					}
				}).returns(Types.INT));

		context = context.map(new RichMapFunction <Params, Params>() {
			private static final long serialVersionUID = -7796590264875170687L;
			IntCounter counter = new IntCounter();

			@Override
			public void open(Configuration parameters) throws Exception {
				getRuntimeContext().addAccumulator(alinkApsBreakAll, this.counter);
			}

			@Override
			public Params map(Params params) throws Exception {
				int numCheckPoint = params.getInteger(alinkApsNumCheckpoint);
				int numIter = params.getIntegerOrDefault(alinkApsNumIter, 1);
				int numMiniBatch = params.getInteger(alinkApsNumMiniBatch);

				int startPos, endPos;

				if (numCheckPoint <= 0) {
					startPos = 0;
					endPos = numMiniBatch * numIter;
				} else {
					int curCheckPoint = params.getInteger(alinkApsCurCheckpoint);
					DistributedInfo distributedInfo = new DefaultDistributedInfo();
					startPos = (int) distributedInfo.startPos(curCheckPoint, numCheckPoint, numMiniBatch * numIter);
					endPos = (int) distributedInfo.localRowCnt(curCheckPoint, numCheckPoint, numMiniBatch * numIter)
						+ startPos;
				}

				int curPos = params.getInteger(alinkApsStepNum) - 1 + startPos;

				LOG.info("taskId:{}, stepNum:{}", getRuntimeContext().getIndexOfThisSubtask(), curPos);

				int curBlock;
				if (curPos >= endPos) {
					curBlock = -1;
				} else if (curPos >= numMiniBatch * numIter) {
					curBlock = -1;
				} else {
					curBlock = curPos % numMiniBatch;
				}

				boolean hasNextBlock = true;

				if (curPos >= endPos - 1) {
					hasNextBlock = false;
				} else if (curPos >= numMiniBatch * numIter - 1) {
					hasNextBlock = false;
				}

				if (endPos >= numMiniBatch * numIter) {
					this.counter.add(1);
				}

				params.set(ALINK_APS_HAS_NEXT_BLOCK, hasNextBlock);
				params.set(ALINK_APS_CUR_BLOCK, curBlock);

				return params;
			}
		});

		return this;
	}

	public DataSet <Params> getCriterion() {
		return context.flatMap(
			new RichFlatMapFunction <Params, Params>() {
				private static final long serialVersionUID = -156438604518301112L;
				IntCounter counter = new IntCounter();

				@Override
				public void open(Configuration parameters) throws Exception {
					getRuntimeContext().addAccumulator(alinkApsBreakAll, this.counter);
				}

				@Override
				public void flatMap(Params value, Collector <Params> out) throws Exception {
					if (value.getBoolOrDefault(alinkApsBreakAll, false)) {
						counter.add(1);
						return;
					}

					if (value.get(ALINK_APS_HAS_NEXT_BLOCK)) {
						out.collect(value);
					}
				}
			});
	}

	private static class AppendParam2Context implements MapFunction <Params, Params> {
		private static final long serialVersionUID = -4020594804812453044L;
		private Params params;

		public AppendParam2Context(Params params) {
			this.params = params;
		}

		@Override
		public Params map(Params value) throws Exception {
			return value.merge(this.params);
		}
	}

	private final static String SCHEMA_PREFIX1 = "a1";
	private final static String SCHEMA_PREFIX2 = "a2";
	private static final TableSchema DEFAULT_MODEL_SCHEMA = new TableSchema(
		new String[] {"alinkmodelid", "alinkmodelinfo", SCHEMA_PREFIX1, SCHEMA_PREFIX2},
		new TypeInformation <?>[] {Types.LONG, Types.STRING, Types.STRING, Types.STRING}
	);

	private final static int MAX_VARCHAR_SIZE = 30000;

	private static int appendStringToModel(String json, List <Row> rows) {
		return appendStringToModel(json, rows, 1);
	}

	private static int appendStringToModel(String json, List <Row> rows, int startIndex) {
		return appendStringToModel(json, rows, startIndex, MAX_VARCHAR_SIZE);
	}

	private static int appendStringToModel(String json, List <Row> rows, int startIndex, int unitSize) {
		if (null == json || json.length() == 0) {
			return startIndex;
		}
		int n = json.length();
		int cur = 0;
		int rowIndex = startIndex;
		while (cur < n) {
			String str = json.substring(cur, Math.min(cur + unitSize, n));
			Row row = new Row(4);
			row.setField(0, (long) rowIndex);
			row.setField(1, str);
			rows.add(row);
			rowIndex++;
			cur += unitSize;
		}
		return rowIndex;
	}

	private static String extractStringFromModel(List <Row> rows) {
		return extractStringFromModel(rows, 1);
	}

	private static String extractStringFromModel(List <Row> rows, int startIndex) {
		int m = rows.size();
		String[] strs = new String[m];
		for (Row row : rows) {
			int idx = ((Long) row.getField(0)).intValue();
			if (idx >= startIndex) {
				strs[idx - startIndex] = (String) row.getField(1);
			}
		}
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < m; i++) {
			if (null == strs[i]) {
				break;
			}
			sbd.append(strs[i]);
		}
		return sbd.toString();
	}

}
