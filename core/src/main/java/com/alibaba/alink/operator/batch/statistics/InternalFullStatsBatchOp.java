package com.alibaba.alink.operator.batch.statistics;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.exceptions.AkIllegalOperationException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.metadata.def.v0.DatasetFeatureStatisticsList;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.StatsVisualizer;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.batch.statistics.utils.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.statistics.FullStats;
import com.alibaba.alink.operator.common.statistics.statistics.FullStatsConverter;
import com.alibaba.alink.operator.common.statistics.statistics.SummaryResultTable;
import com.alibaba.alink.params.statistics.HasStatLevel_L1.StatLevel;
import com.alibaba.alink.params.statistics.HasTableNames;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

@SuppressWarnings({"UnusedReturnValue", "unused"})
@Internal
public class InternalFullStatsBatchOp extends BatchOperator <InternalFullStatsBatchOp> implements HasTableNames <InternalFullStatsBatchOp> {

	public InternalFullStatsBatchOp() {
		this(new Params());
	}

	public InternalFullStatsBatchOp(Params params) {
		super(params);
	}

	@Override
	public InternalFullStatsBatchOp linkFrom(BatchOperator <?>... inputs) {
		AkPreconditions.checkArgument(inputs.length > 0,
			new AkIllegalOperationException("Must provide at least 1 inputs."));
		int n = inputs.length;
		DataSet <Tuple2 <Integer, SummaryResultTable>> unionSrtDataSet = null;
		for (int i = 0; i < n; i += 1) {
			final int index = i;
			DataSet <SummaryResultTable> srtDataSet = StatisticsHelper.getSRT(inputs[i], StatLevel.L3);
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

		String[] tableNames = new String[n];
		for (int i = 0; i < n; i++) {
			tableNames[i] = "table" + String.valueOf(i + 1);
		}
		if (getParams().contains(HasTableNames.TABLE_NAMES)) {
			String[] inputNames = getTableNames();
			for (int i = 0; i < Math.min(n, inputNames.length); i++) {
				tableNames[i] = inputNames[i];
			}
		}

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
		AkPreconditions.checkState(null != this.getOutputTable(),
			new AkIllegalOperationException("Please call link from/to before collect statistics."));
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
		return lazyVizFullStats(newTableNames, false);
	}

	@Internal
	public final InternalFullStatsBatchOp lazyVizFullStats(String[] newTableNames, boolean useExperimentalViz) {
		//noinspection Convert2Lambda
		return lazyCollectFullStats(new Consumer <FullStats>() {
			@Override
			public void accept(FullStats fullStats) {
				StatsVisualizer visualizer = StatsVisualizer.getInstance();
				DatasetFeatureStatisticsList datasetFeatureStatisticsList = fullStats.getDatasetFeatureStatisticsList();
				if (useExperimentalViz) {
					visualizer.visualizeNew(datasetFeatureStatisticsList, newTableNames);
				} else {
					visualizer.visualize(datasetFeatureStatisticsList, newTableNames);
				}
			}
		});
	}
}
