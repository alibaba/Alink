package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ShuffleBatchOp;
import com.alibaba.alink.params.recommendation.NegativeItemSamplingParams;
import org.apache.commons.lang3.ArrayUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Given a dataset of user-item pairs, generate new user-item pairs and add them to original dataset. For the user in
 * each user-item pair, a "SAMPLING_FACTOR" number of items are sampled.
 */
@InputPorts(values = {
	@PortSpec(PortType.DATA)
})
@OutputPorts(values = {
	@PortSpec(PortType.DATA)
})
@NameCn("推荐负采样")
public final class NegativeItemSamplingBatchOp
	extends BatchOperator <NegativeItemSamplingBatchOp>
	implements NegativeItemSamplingParams <NegativeItemSamplingBatchOp> {

	private static final long serialVersionUID = 1296665548360617576L;

	public NegativeItemSamplingBatchOp() {
		this(new Params());
	}

	public NegativeItemSamplingBatchOp(Params params) {
		super(params);
	}

	@Override
	public NegativeItemSamplingBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> userItemPairs = inputs[0];
		setMLEnvironmentId(userItemPairs.getMLEnvironmentId());
		Preconditions.checkArgument(userItemPairs.getColNames().length == 2);
		BatchOperator <?> distinctItems = userItemPairs.select(userItemPairs.getColNames()[1]).distinct();
		negativeSampling(userItemPairs, distinctItems);
		this.setOutputTable(this.link(new ShuffleBatchOp()).getOutputTable());
		return this;
	}

	private static DataSet <Tuple2 <Object, Object>> getUserItemDataSet(BatchOperator <?> data) {
		return data.getDataSet()
			.map(new MapFunction <Row, Tuple2 <Object, Object>>() {
				private static final long serialVersionUID = -2086770134528760473L;

				@Override
				public Tuple2 <Object, Object> map(Row value) {
					Object u = value.getField(0);
					Object i = value.getField(1);
					return Tuple2.of(u, i);
				}
			});
	}

	/**
	 * For each user in user-item pair in data, sample negative items from history data.
	 */
	private void negativeSampling(BatchOperator <?> data, BatchOperator <?> distinctItems) {
		final int samplingFactor = getSamplingFactor();
		Preconditions.checkArgument(data.getColNames().length == 2);
		Preconditions.checkArgument(distinctItems.getColNames().length == 1);
		DataSet <Tuple2 <Object, Object>> historyUserItem = getUserItemDataSet(data);

		DataSet <Object> items = distinctItems.getDataSet()
			.map(new MapFunction <Row, Object>() {
				private static final long serialVersionUID = -8648184004287735175L;

				@Override
				public Object map(Row value) {
					return value.getField(0);
				}
			});

		DataSet <Tuple3 <Object, Object, Long>> sampled = historyUserItem
			.map(new MapFunction <Tuple2 <Object, Object>, Tuple3 <String, Object, Object>>() {
				private static final long serialVersionUID = -6957327460225823558L;

				@Override
				public Tuple3 <String, Object, Object> map(Tuple2 <Object, Object> value) {
					return Tuple3.of(String.valueOf(value.f0), value.f0, value.f1);
				}
			})
			.groupBy(0)
			.reduceGroup(
				new RichGroupReduceFunction <Tuple3 <String, Object, Object>, Tuple3 <Object, Object, Long>>() {
					private static final long serialVersionUID = 306722066512456784L;
					transient List <Long> candidates;
					transient Random random;

					@Override
					public void open(Configuration parameters) {
						random = new Random(getRuntimeContext().getIndexOfThisSubtask());
						candidates = getRuntimeContext().getBroadcastVariable("items");
					}

					@Override
					public void reduce(Iterable <Tuple3 <String, Object, Object>> values,
									   Collector <Tuple3 <Object, Object, Long>> out) {
						Set <Object> items = new HashSet <>();
						Object user = null;
						long cnt = 0;
						for (Tuple3 <String, Object, Object> v : values) {
							user = v.f1;
							items.add(v.f2);
							cnt++;
							out.collect(Tuple3.of(user, v.f2, 1L));
						}

						final int maxTry = 32;
						for (int i = 0; i < cnt * samplingFactor; i++) {
							for (int j = 0; j < maxTry; j++) {
								int which = random.nextInt(candidates.size());
								Object picked = candidates.get(which);
								if (!items.contains(picked)) {
									out.collect(Tuple3.of(user, picked, 0L));
									break;
								}
							}
						}
					}
				})
			.withBroadcastSet(items, "items")
			.name("negative_sampling");

		DataSet <Row> output = sampled
			.map(new MapFunction <Tuple3 <Object, Object, Long>, Row>() {
				private static final long serialVersionUID = -9124354562578678385L;

				@Override
				public Row map(Tuple3 <Object, Object, Long> value) {
					return Row.of(value.f0, value.f1, value.f2);
				}
			});

		String[] fieldNames = ArrayUtils.add(data.getColNames(), "label");
		TypeInformation <?>[] fieldTypes = ArrayUtils.add(data.getColTypes(), Types.LONG);
		setOutput(output, new TableSchema(fieldNames, fieldTypes));
	}
}
