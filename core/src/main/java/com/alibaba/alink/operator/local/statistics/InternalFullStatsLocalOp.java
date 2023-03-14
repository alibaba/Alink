package com.alibaba.alink.operator.local.statistics;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.exceptions.AkIllegalOperationException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.metadata.def.v0.DatasetFeatureStatisticsList;
import com.alibaba.alink.operator.batch.utils.StatsVisualizer;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.common.statistics.statistics.WindowTable;
import com.alibaba.alink.operator.common.statistics.statistics.FullStats;
import com.alibaba.alink.operator.common.statistics.statistics.FullStatsConverter;
import com.alibaba.alink.operator.common.statistics.statistics.SrtUtil;
import com.alibaba.alink.operator.common.statistics.statistics.SummaryResultTable;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.statistics.HasStatLevel_L1;
import com.alibaba.alink.params.statistics.HasStatLevel_L1.StatLevel;
import com.alibaba.alink.params.statistics.HasTableNames;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

@SuppressWarnings({"UnusedReturnValue", "unused"})
@Internal
public class InternalFullStatsLocalOp extends LocalOperator <InternalFullStatsLocalOp>
	implements HasTableNames <InternalFullStatsLocalOp> {

	public InternalFullStatsLocalOp() {
		this(new Params());
	}

	public InternalFullStatsLocalOp(Params params) {
		super(params);
	}

	SummaryResultTable srt(LocalOperator <?> in, HasStatLevel_L1.StatLevel statLevel) {
		TypeInformation <?>[] colTypeInfos = in.getColTypes();
		Class[] colTypes = new Class[colTypeInfos.length];
		for (int i = 0; i < colTypeInfos.length; i++) {
			colTypes[i] = colTypeInfos[i].getTypeClass();
		}
		WindowTable wt = new WindowTable(in.getColNames(), colTypes, in.getOutputTable().getRows());
		SummaryResultTable srt = SrtUtil.batchSummary(wt, in.getColNames(),
			10, 10, 1000, 10, statLevel);
		return srt;
	}

	@Override
	public InternalFullStatsLocalOp linkFrom(LocalOperator <?>... inputs) {
		AkPreconditions.checkArgument(inputs.length > 0,
			new AkIllegalOperationException("Must provide at least 1 inputs."));
		int n = inputs.length;
		DataSet <Tuple2 <Integer, SummaryResultTable>> unionSrtDataSet = null;
		//noinspection unchecked
		Tuple2 <Integer, SummaryResultTable>[] srts = new Tuple2[n];
		for (int i = 0; i < n; i += 1) {
			srts[i] = Tuple2.of(i, srt(inputs[i], StatLevel.L3));
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
		String[] colTypeStrs = FlinkTypeConverter.getTypeString(colTypes);
		// assume all datasets have same schemas
		FullStats fullStats = FullStats.fromSummaryResultTable(tableNames, colTypeStrs, Arrays.asList(srts));

		List <Row> out = new ArrayList <>();
		ListCollector <Row> collector = new ListCollector <>(out);
		new FullStatsConverter().save(fullStats, collector);

		setOutputTable(new MTable(out, new FullStatsConverter().getModelSchema()));
		return this;
	}

	public FullStats collectFullStats() {
		AkPreconditions.checkState(null != this.getOutputTable(),
			new AkIllegalOperationException("Please call link from/to before collect statistics."));
		return new FullStatsConverter().load(collect());
	}

	public final InternalFullStatsLocalOp lazyCollectFullStats(
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
	public final InternalFullStatsLocalOp lazyCollectFullStats(Consumer <FullStats>... callbacks) {
		return lazyCollectFullStats(Arrays.asList(callbacks));
	}

	public final InternalFullStatsLocalOp lazyVizFullStats() {
		return lazyVizFullStats(null);
	}

	public final InternalFullStatsLocalOp lazyVizFullStats(String[] newTableNames) {
		return lazyVizFullStats(newTableNames, false);
	}

	@Internal
	public final InternalFullStatsLocalOp lazyVizFullStats(String[] newTableNames, boolean useExperimentalViz) {
		//noinspection Convert2Lambda
		return lazyCollectFullStats(new Consumer <FullStats>() {
			@Override
			public void accept(FullStats fullStats) {
				StatsVisualizer visualizer = StatsVisualizer.getInstance();
				DatasetFeatureStatisticsList datasetFeatureStatisticsList =
					fullStats.getDatasetFeatureStatisticsList();
				if (useExperimentalViz) {
					visualizer.visualizeNew(datasetFeatureStatisticsList, newTableNames);
				} else {
					visualizer.visualize(datasetFeatureStatisticsList, newTableNames);
				}
			}
		});
	}
}
