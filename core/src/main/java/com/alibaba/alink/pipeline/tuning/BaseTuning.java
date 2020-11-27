package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.io.directreader.DefaultDistributedInfo;
import com.alibaba.alink.common.io.directreader.DistributedInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo;
import com.alibaba.alink.common.lazy.LazyEvaluation;
import com.alibaba.alink.common.lazy.LazyObjectsManager;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.pipeline.EstimatorBase;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.TransformerBase;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * BaseTuning.
 */
public abstract class BaseTuning<T extends BaseTuning <T, M>, M extends BaseTuningModel <M>>
	extends EstimatorBase <T, M> implements HasLazyPrintTrainInfo <T> {

	private static final long serialVersionUID = 7100530176503587968L;
	private EstimatorBase <?, ?> estimator;
	private TuningEvaluator <?> tuningEvaluator;

	public BaseTuning() {
		super();
	}

	public EstimatorBase <?, ?> getEstimator() {
		return estimator;
	}

	public T setEstimator(EstimatorBase <?, ?> value) {
		this.estimator = value;
		return (T) this;
	}

	public T setTuningEvaluator(TuningEvaluator <?> tuningEvaluator) {
		this.tuningEvaluator = tuningEvaluator;
		return (T) this;
	}

	@Override
	public M fit(BatchOperator <?> input) {
		Tuple2 <TransformerBase, Report> result = tuning(input);

		if (getParams().get(LAZY_PRINT_TRAIN_INFO_ENABLED)) {
			final String title = getParams().get(LAZY_PRINT_TRAIN_INFO_TITLE);
			LazyObjectsManager lazyObjectsManager = LazyObjectsManager.getLazyObjectsManager(this);
			LazyEvaluation <Report> lazyReport = lazyObjectsManager.genLazyReport(this);
			lazyReport.addCallback(report -> {
				if (title != null) {
					System.out.println(title);
				}
				System.out.println(report.toString());
			});
			lazyReport.addValue(result.f1);
		}
		return createModel(result.f0);
	}

	@Override
	public M fit(StreamOperator <?> input) {
		throw new UnsupportedOperationException("Tuning on stream not supported.");
	}

	private M createModel(TransformerBase transformer) {
		try {
			ParameterizedType pt =
				(ParameterizedType) this.getClass().getGenericSuperclass();

			Class <M> classM = (Class <M>) pt.getActualTypeArguments()[1];

			return classM.getConstructor(TransformerBase.class)
				.newInstance(transformer);

		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}

	}

	protected abstract Tuple2 <TransformerBase, Report> tuning(BatchOperator <?> in);

	protected Tuple2 <Pipeline, Report> findBestTVSplit(
		BatchOperator <?> in, double ratio, PipelineCandidatesBase candidates) {
		int nIter = candidates.size();

		SplitBatchOp sbo = new SplitBatchOp()
			.setFraction(ratio)
			.setMLEnvironmentId(getMLEnvironmentId())
			.linkFrom(
				new TableSourceBatchOp(
					DataSetConversionUtil.toTable(
						in.getMLEnvironmentId(),
						shuffle(in.getDataSet()),
						in.getSchema()
					)
				)
				.setMLEnvironmentId(getMLEnvironmentId())
			);

		int bestIdx = -1;
		double bestMetric = 0.;
		ArrayList <Double> experienceScores = new ArrayList <>(nIter);
		List <Report.ReportElement> reportElements = new ArrayList <>();

		for (int i = 0; i < nIter; i++) {
			Tuple2 <Pipeline, List <Tuple3 <Integer, ParamInfo, Object>>> cur;
			try {
				cur = candidates.get(i, experienceScores);
			} catch (CloneNotSupportedException e) {
				throw new RuntimeException(e);
			}

			double metric = Double.NaN;
			try {
				metric = tuningEvaluator.evaluate(cur.f0
					.fit(sbo)
					.transform(sbo.getSideOutput(0))
				);
			} catch (Exception ex) {
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println(String.format("BestTVSplit, i: %d, best: %f, metric: %f, exception: %s",
						i, bestMetric, metric, ExceptionUtils.stringifyException(ex)));
				}

				experienceScores.add(i, metric);

				reportElements.add(
					new Report.ReportElement(
						cur.f0,
						cur.f1,
						metric,
						ExceptionUtils.stringifyException(ex)
					)
				);

				continue;
			}

			experienceScores.add(i, metric);

			if (Double.isNaN(metric)) {
				reportElements.add(
					new Report.ReportElement(
						cur.f0,
						cur.f1,
						metric,
						"Metric is nan."
					)
				);

				continue;
			}

			reportElements.add(
				new Report.ReportElement(
					cur.f0,
					cur.f1,
					metric
				)
			);

			if (bestIdx == -1) {
				bestMetric = metric;
				bestIdx = i;
			} else {
				if ((tuningEvaluator.isLargerBetter() && bestMetric < metric)
					|| (!tuningEvaluator.isLargerBetter() && bestMetric > metric)) {
					bestMetric = metric;
					bestIdx = i;
				}
			}

			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(String.format("BestTVSplit, i: %d, best: %f, metric: %f",
					i, bestMetric, metric));
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

	protected Tuple2 <Pipeline, Report> findBestCV(BatchOperator <?> in, int k, PipelineCandidatesBase candidates) {
		Preconditions.checkArgument(k > 1, "numFolds could be greater than 1.");
		DataSet <Tuple2 <Integer, Row>> splitData = split(in, k);

		int nIter = candidates.size();
		Double bestAvg = null;
		Integer bestIdx = null;

		ArrayList <Double> experienceScores = new ArrayList <>(nIter);
		List <Report.ReportElement> reportElements = new ArrayList <>();
		for (int i = 0; i < nIter; i++) {
			Tuple2 <Pipeline, List <Tuple3 <Integer, ParamInfo, Object>>> cur;
			try {
				cur = candidates.get(i, experienceScores);
			} catch (CloneNotSupportedException e) {
				throw new RuntimeException(e);
			}

			Tuple2 <Double, String> avg = kFoldCv(splitData, cur.f0, in.getSchema(), k);

			experienceScores.add(i, avg.f0);

			if (Double.isNaN(avg.f0)) {
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println(String.format("BestCV, i: %d, best: %f, avg: %f",
						i, bestAvg, avg.f0));
				}
				reportElements.add(
					new Report.ReportElement(
						cur.f0,
						cur.f1,
						avg.f0,
						avg.f1
					)
				);
				continue;
			}

			reportElements.add(
				new Report.ReportElement(
					cur.f0,
					cur.f1,
					avg.f0,
					avg.f1
				)
			);

			if (bestAvg == null) {
				bestAvg = avg.f0;
				bestIdx = i;
			} else if ((tuningEvaluator.isLargerBetter() && bestAvg < avg.f0)
				|| (!tuningEvaluator.isLargerBetter() && bestAvg > avg.f0)) {
				bestAvg = avg.f0;
				bestIdx = i;
			}

			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(
					String.format(
						"BestCV, i: %d, best: %f, avg: %f",
						i, bestAvg, avg.f0
					)
				);
			}
		}

		if (bestIdx == null) {
			throw new RuntimeException(
				"Can not find a best model. Report: "
					+ new Report(tuningEvaluator, reportElements).toPrettyJson()
			);
		}

		try {
			return Tuple2.of(candidates.get(bestIdx, experienceScores).f0, new Report(tuningEvaluator,
				reportElements));
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}

	private Tuple2 <Double, String> kFoldCv(
		DataSet <Tuple2 <Integer, Row>> splitData,
		Pipeline pipeline,
		TableSchema schema,
		int k) {
		double ret = 0.;
		int validSize = 0;

		StringBuilder reason = new StringBuilder();

		for (int i = 0; i < k; ++i) {
			final int loop = i;
			DataSet <Row> trainInput = splitData
				.filter(new FilterFunction <Tuple2 <Integer, Row>>() {
					private static final long serialVersionUID = 2249884521437544236L;

					@Override
					public boolean filter(Tuple2 <Integer, Row> value) {
						return value.f0 != loop;
					}
				})
				.map(new MapFunction <Tuple2 <Integer, Row>, Row>() {
					private static final long serialVersionUID = 2618229645786221757L;

					@Override
					public Row map(Tuple2 <Integer, Row> value) {
						return value.f1;
					}
				});

			DataSet <Row> testInput = splitData
				.filter(new FilterFunction <Tuple2 <Integer, Row>>() {
					private static final long serialVersionUID = 5811166054549336470L;

					@Override
					public boolean filter(Tuple2 <Integer, Row> value) {
						return value.f0 == loop;
					}
				})
				.map(new MapFunction <Tuple2 <Integer, Row>, Row>() {
					private static final long serialVersionUID = -1760709990316111721L;

					@Override
					public Row map(Tuple2 <Integer, Row> value) {
						return value.f1;
					}
				});

			PipelineModel model = pipeline
				.fit(
					new TableSourceBatchOp(
						DataSetConversionUtil
							.toTable(getMLEnvironmentId(), trainInput, schema)
					)
					.setMLEnvironmentId(getMLEnvironmentId())
				);

			double localMetric = Double.NaN;
			try {
				localMetric = tuningEvaluator
					.evaluate(
						model.transform(new TableSourceBatchOp(
							DataSetConversionUtil.toTable(getMLEnvironmentId(), testInput, schema)))
					);
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println(String.format("kFoldCv, k: %d, i: %d, metric: %f",
						k, i, localMetric));
				}
			} catch (Exception ex) {
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println(
						String.format("kFoldCv err, k: %d, i: %d, metric: %f, exception: %s",
							k, i, localMetric, ExceptionUtils.stringifyException(ex)));
				}

				reason.append(ExceptionUtils.stringifyException(ex)).append("\n");

				continue;
			}

			ret += localMetric;
			validSize++;
		}

		if (validSize == 0) {
			reason.append("valid size is zero.").append("\n");

			return Tuple2.of(Double.NaN, reason.toString());
		}

		ret /= validSize;

		if (validSize > 0) {
			return Tuple2.of(ret, reason.toString());
		} else {
			reason.append("valid size if negative.").append("\n");

			return Tuple2.of(Double.NaN, reason.toString());
		}
	}

	private DataSet <Row> shuffle(DataSet <Row> input) {
		return input
			.map(new MapFunction <Row, Tuple2 <Long, Row>>() {
				private static final long serialVersionUID = 2565906511879493627L;

				@Override
				public Tuple2 <Long, Row> map(Row value) {
					return Tuple2.of(
						ThreadLocalRandom.current().nextLong(Long.MAX_VALUE), value
					);
				}
			})
			.partitionCustom(new Partitioner <Long>() {
				private static final long serialVersionUID = 8626504946902766931L;

				@Override
				public int partition(Long key, int numPartitions) {
					return (int) (key % numPartitions);
				}
			}, 0)
			.sortPartition(0, Order.ASCENDING)
			.map(new MapFunction <Tuple2 <Long, Row>, Row>() {
				private static final long serialVersionUID = 2667225910228407097L;

				@Override
				public Row map(Tuple2 <Long, Row> value) {
					return value.f1;
				}
			});
	}

	private DataSet <Tuple2 <Integer, Row>> split(BatchOperator <?> data, int k) {

		DataSet <Row> input = shuffle(data.getDataSet());

		DataSet <Tuple2 <Integer, Long>> counts = DataSetUtils.countElementsPerPartition(input);

		return input
			.mapPartition(new RichMapPartitionFunction <Row, Tuple2 <Integer, Row>>() {
				private static final long serialVersionUID = -902599228310615694L;
				long taskStart = 0L;
				long totalNumInstance = 0L;

				@Override
				public void open(Configuration parameters) {
					List <Tuple2 <Integer, Long>> counts1 = getRuntimeContext().getBroadcastVariable("counts");

					int taskId = getRuntimeContext().getIndexOfThisSubtask();
					for (Tuple2 <Integer, Long> cnt : counts1) {

						if (taskId < cnt.f0) {
							taskStart += cnt.f1;
						}

						totalNumInstance += cnt.f1;
					}
				}

				@Override
				public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Integer, Row>> out) {
					DistributedInfo distributedInfo = new DefaultDistributedInfo();
					Tuple2 <Integer, Long> split1 = new Tuple2 <>(-1, -1L);
					long lcnt = taskStart;

					for (int i = 0; i <= k; ++i) {
						long sp = distributedInfo.startPos(i, k, totalNumInstance);
						long lrc = distributedInfo.localRowCnt(i, k, totalNumInstance);

						if (taskStart < sp) {
							split1.f0 = i - 1;
							split1.f1 = distributedInfo.startPos(i - 1, k, totalNumInstance)
								+ distributedInfo.localRowCnt(i - 1, k, totalNumInstance);

							break;
						}

						if (taskStart == sp) {
							split1.f0 = i;
							split1.f1 = sp + lrc;

							break;
						}
					}

					for (Row val : values) {

						if (lcnt >= split1.f1) {
							split1.f0 += 1;
							split1.f1 = distributedInfo.localRowCnt(split1.f0, k, totalNumInstance) + lcnt;
						}

						out.collect(Tuple2.of(split1.f0, val));
						lcnt++;
					}
				}
			}).withBroadcastSet(counts, "counts");
	}
}
