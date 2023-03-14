package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.directreader.DefaultDistributedInfo;
import com.alibaba.alink.common.io.directreader.DistributedInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.MemSourceLocalOp;
import com.alibaba.alink.params.tuning.HasTrainRatio;
import com.alibaba.alink.pipeline.EstimatorBase;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.tuning.Report.ReportElement;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

@NameCn("Bayes搜索TV")
public class BayesSearchTVSplit
	extends BaseBayesSearch <BayesSearchTVSplit, BayesSearchTVSplitModel>
	implements HasTrainRatio <BayesSearchTVSplit> {

	private static final long serialVersionUID = 5726020877861859001L;

	public BayesSearchTVSplit() {
		super();
	}

	@Override
	protected Tuple2 <Pipeline, Report> findBest(BatchOperator <?> in, PipelineCandidatesBayes candidates) {
		final int nIter = candidates.size();
		final double ratio = getTrainRatio();
		TuningEvaluator tuningEvaluator = getTuningEvaluator();
		EstimatorBase estimator = getEstimator();
		Params tuningParams = tuningEvaluator.getParams();
		final Class tuningClass = tuningEvaluator.getClass();
		Params estimatorParams = estimator.getParams();

		List <ReportElement> reportElements = new ArrayList <>();
		ArrayList <Double> experienceScores = new ArrayList <>(nIter);

		if (getParallelTuningMode()) {
			for (int i = 0; i < nIter; ++i) {
				reportElements.add(null);
			}

			final TableSchema dataSchema = in.getSchema();

			final String[] dataFieldNames = dataSchema.getFieldNames();
			final DataType[] dataFieldTypes = dataSchema.getFieldDataTypes();

			List <Row> rows = null;
			try {
				rows = in.rebalance().getDataSet()
					.mapPartition(new RichMapPartitionFunction <Row, Tuple3 <Integer, Integer, Row>>() {
						@Override
						public void mapPartition(Iterable <Row> values, Collector <Tuple3 <Integer, Integer, Row>> out)
							throws Exception {
							int numTask = getRuntimeContext().getNumberOfParallelSubtasks();
							Random random = new Random();
							for (Row row : values) {
								for (int i = 0; i < numTask; ++i) {
									if (ThreadLocalRandom.current().nextDouble() <= ratio) {
										//if (random.nextDouble() <= ratio) {
										out.collect(Tuple3.of(i, 0, row));
									} else {
										out.collect(Tuple3.of(i, 1, row));
									}
								}
							}
						}
					})
					.partitionCustom(new Partitioner <Integer>() {
						@Override
						public int partition(Integer key, int numPartitions) {
							return (int) (key % numPartitions);
						}
					}, 0)
					.mapPartition(new RichMapPartitionFunction <Tuple3 <Integer, Integer, Row>, Row>() {

						@Override
						public void mapPartition(Iterable <Tuple3 <Integer, Integer, Row>> values, Collector <Row> out)
							throws Exception {

							List <Row> trainSet = new ArrayList <>();
							List <Row> validateSet = new ArrayList <>();

							for (Tuple3 <Integer, Integer, Row> val : values) {
								if (val.f1 == 0) {
									trainSet.add(val.f2);
								} else {
									validateSet.add(val.f2);
								}
							}

							int taskId = getRuntimeContext().getIndexOfThisSubtask();
							int numTask = getRuntimeContext().getNumberOfParallelSubtasks();

							DistributedInfo distributedInfo = new DefaultDistributedInfo();

							int start = (int) distributedInfo.startPos(taskId, numTask, nIter);
							int cnt = (int) distributedInfo.localRowCnt(taskId, numTask, nIter);

							LocalOperator trainDataLocalOp = new MemSourceLocalOp(trainSet,
								TableSchema.builder().fields(dataFieldNames, dataFieldTypes).build());
							LocalOperator testDataLocalOp = new MemSourceLocalOp(validateSet,
								TableSchema.builder().fields(dataFieldNames, dataFieldTypes).build());

							if (trainDataLocalOp.getOutputTable().getNumRow() == 0) {
								trainDataLocalOp = testDataLocalOp;
							}
							if (testDataLocalOp.getOutputTable().getNumRow() == 0) {
								testDataLocalOp = trainDataLocalOp;
							}

							List <Double> localExperienceScores = new ArrayList <>();
							for (int i = start; i < start + cnt; ++i) {
								Tuple2 <Pipeline, List <Tuple3 <Integer, ParamInfo, Object>>> candidate
									= candidates.get(i, localExperienceScores);
								Pipeline pipeline = candidate.f0;

								TuningEvaluator tuning = (TuningEvaluator) tuningClass.getConstructor(Params.class)
									.newInstance(tuningParams);
								PipelineModel model = pipeline.fit(trainDataLocalOp);
								double testMetric = tuning.evaluate(model.transform(testDataLocalOp));
								double trainMetric = tuning.evaluate(model.transform(trainDataLocalOp));
								localExperienceScores.add(testMetric);
								if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
									System.out.println(String.format(
										"Iter %d in group %d, taskId:%d params:%s trainMetric:%f testMetric:%f",
										i - start, taskId, i, pipeline.get(pipeline.size() - 1).getParams().toString(),
										trainMetric,
										testMetric));
								}
								out.collect(Row.of(i, taskId, pipeline, testMetric, candidate.f1));
							}
						}
					}).name("parallel_standalone_build_model")
					.collect();
			} catch (Exception e) {
				e.printStackTrace();
			}
			for (Row row : rows) {
				Integer index = (Integer) row.getField(0);
				Integer taskId = (Integer) row.getField(1);
				Pipeline pipeline = (Pipeline) row.getField(2);
				Double metric = (Double) row.getField(3);
				List <Tuple3 <Integer, ParamInfo, Object>> stageParamInfos = (List <Tuple3 <Integer, ParamInfo,
					Object>>) row.getField(4);
				candidates.checkParamsSameValueOrNullValue(index, stageParamInfos);
				reportElements.set(index, new ReportElement(pipeline, stageParamInfos, metric));
			}
		} else {
			return findBestTVSplit(in, getTrainRatio(), candidates);
		}
		int bestIdx = -1;
		double bestMetric = 0.;
		for (int i = 0; i < reportElements.size(); i++) {
			Report.ReportElement report = reportElements.get(i);
			experienceScores.add(i, report.getMetric());
			if (bestIdx == -1) {
				bestMetric = report.getMetric();
				bestIdx = i;
			} else {
				if ((tuningEvaluator.isLargerBetter() && bestMetric < report.getMetric())
					|| (!tuningEvaluator.isLargerBetter() && bestMetric > report.getMetric())) {
					bestMetric = report.getMetric();
					bestIdx = i;
				}
			}

			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(String.format("BestTVSplit, i: %d, best: %f, metric: %f",
					i, bestMetric, report.getMetric()));
			}
		}

		if (bestIdx < 0) {
			throw new RuntimeException(
				"Can not find a best model. Report: "
					+ new Report(tuningEvaluator, reportElements).toPrettyJson()
			);
		}

		try {
			return Tuple2.of(
				candidates.get(bestIdx, experienceScores).f0,
				new Report(tuningEvaluator, reportElements)
			);
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected Tuple2 <Pipeline, Report> findBest(LocalOperator <?> in, PipelineCandidatesBayes candidates) {
		return findBestTVSplit(in, getTrainRatio(), candidates);
	}
}
