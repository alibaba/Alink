package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.getRegressionStatistics;

/**
 * Process the streaming data within a time interval in EvalRegressionStreamOp.
 * Extract info from the predResult column, and generate RegressionMetrics.
 */
public class Regression implements AllWindowFunction <Row, Row, TimeWindow> {
	private static final long serialVersionUID = 8539171328124159299L;

	@Override
	public void apply(TimeWindow timeWindow, Iterable <Row> rows, Collector <Row> collector) throws Exception {
		RegressionMetricsSummary metrics = getRegressionStatistics(rows);
		collector.collect(metrics.toMetrics().serialize());
	}
}
