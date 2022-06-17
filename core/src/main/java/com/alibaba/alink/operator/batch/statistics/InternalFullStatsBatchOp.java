package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.metadata.def.v0.DatasetFeatureStatisticsList;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.StatsVisualizer;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.statistics.StatisticUtil;
import com.alibaba.alink.operator.common.statistics.statistics.FullStats;
import com.alibaba.alink.operator.common.statistics.statistics.FullStatsConverter;
import com.alibaba.alink.operator.common.statistics.statistics.SummaryResultTable;
import com.alibaba.alink.params.statistics.HasStatLevel_L1.StatLevel;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

@SuppressWarnings({"UnusedReturnValue", "unused"})
@Internal
public class InternalFullStatsBatchOp extends BatchOperator <InternalFullStatsBatchOp> {

	public InternalFullStatsBatchOp() {
		this(new Params());
	}

	public InternalFullStatsBatchOp(Params params) {
		super(params);
	}

	@Override
	public InternalFullStatsBatchOp linkFrom(BatchOperator <?>... inputs) {
		Preconditions.checkArgument(inputs.length > 0, "Must provide at least 1 inputs.");
		int n = inputs.length;
		DataSet <Tuple2 <Integer, SummaryResultTable>> unionSrtDataSet = null;
		for (int i = 0; i < n; i += 1) {
			final int index = i;
			DataSet <SummaryResultTable> srtDataSet = StatisticUtil.getSRT(inputs[i], StatLevel.L3);
			//noinspection Convert2Lambda
			DataSet <Tuple2 <Integer, SummaryResultTable>> indexedSrtDataSet = srtDataSet
				.map(new MapFunction <SummaryResultTable, Tuple2 <Integer, SummaryResultTable>>() {
					@Override
					public Tuple2 <Integer, SummaryResultTable> map(SummaryResultTable value) {
						return Tuple2.of(index, value);
					}
				});
			unionSrtDataSet = null == unionSrtDataSet
				? indexedSrtDataSet
				: unionSrtDataSet.union(indexedSrtDataSet);
		}
		String[] tableNames = Arrays.stream(inputs)
			.map(d -> d.getOutputTable().toString())
			.toArray(String[]::new);
		// assume all datasets have same schemas
		final TypeInformation <?>[] colTypes = inputs[0].getColTypes();
		//noinspection Convert2Lambda
		DataSet <Row> srtJsonDataSet = unionSrtDataSet
			.reduceGroup(new GroupReduceFunction <Tuple2 <Integer, SummaryResultTable>, Row>() {
				@Override
				public void reduce(Iterable <Tuple2 <Integer, SummaryResultTable>> values, Collector <Row> out) {
					String[] colTypeStrs = FlinkTypeConverter.getTypeString(colTypes);
					// assume all datasets have same schemas
					FullStats fullStats = FullStats.fromSummaryResultTable(tableNames, colTypeStrs, values);
					new FullStatsConverter().save(fullStats, out);
				}
			});
		setOutput(srtJsonDataSet, new FullStatsConverter().getModelSchema());
		return this;
	}

	public FullStats collectFullStats() {
		Preconditions.checkArgument(null != this.getOutputTable(), "Please link from or link to.");
		return new FullStatsConverter().load(collect());
	}

	public final InternalFullStatsBatchOp lazyCollectFullStats(
		List <Consumer <FullStats>> callbacks) {
		this.lazyCollect(d -> {
			FullStats fullStats = new FullStatsConverter().load(d);
			for (Consumer <FullStats> callback : callbacks) {
				callback.accept(fullStats);
			}
		});
		return this;
	}

	@SafeVarargs
	public final InternalFullStatsBatchOp lazyCollectFullStats(Consumer <FullStats>... callbacks) {
		return lazyCollectFullStats(Arrays.asList(callbacks));
	}

	public final InternalFullStatsBatchOp lazyVizFullStats() {
		return lazyVizFullStats(null);
	}

	public final InternalFullStatsBatchOp lazyVizFullStats(String[] newTableNames) {
		//noinspection Convert2Lambda
		return lazyCollectFullStats(new Consumer <FullStats>() {
			@Override
			public void accept(FullStats fullStats) {
				StatsVisualizer visualizer = StatsVisualizer.getInstance();
				DatasetFeatureStatisticsList datasetFeatureStatisticsList = fullStats.getDatasetFeatureStatisticsList();
				if (null != newTableNames && newTableNames.length > 0) {
					assert datasetFeatureStatisticsList.getDatasetsCount() == newTableNames.length;
					DatasetFeatureStatisticsList.Builder builder = datasetFeatureStatisticsList.toBuilder();
					for (int i = 0; i < newTableNames.length; i += 1) {
						builder.getDatasetsBuilder(i).setName(newTableNames[i]);
					}
					datasetFeatureStatisticsList = builder.build();
				}
				visualizer.visualize(datasetFeatureStatisticsList);
			}
		});
	}
}
