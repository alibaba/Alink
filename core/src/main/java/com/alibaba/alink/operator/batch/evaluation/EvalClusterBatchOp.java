package com.alibaba.alink.operator.batch.evaluation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.evaluation.BaseMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.ClusterEvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.ClusterMetrics;
import com.alibaba.alink.operator.common.evaluation.ClusterMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.EvaluationMetricsCollector;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.LongMatrix;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.operator.common.statistics.basicstatistic.SparseVectorSummary;
import com.alibaba.alink.params.evaluation.EvalClusterParams;

import java.util.List;
import java.util.Map;

/**
 * Calculate the cluster evaluation metrics for clustering.
 * <p>
 * PredictionCol is required for evaluation. LabelCol is optional, if given, NMI/Purity/RI/ARI will be calcuated.
 * VectorCol is also optional, if given, SilhouetteCoefficient/SSB/SSW/Compactness/SP/DB
 * /VRC will be calculated. If only predictionCol is given, only K/ClusterArray/CountArray will be
 * calculated.
 */
public final class EvalClusterBatchOp extends BatchOperator<EvalClusterBatchOp>
    implements EvalClusterParams<EvalClusterBatchOp>, EvaluationMetricsCollector<ClusterMetrics, EvalClusterBatchOp> {
    public static final String SILHOUETTE_COEFFICIENT = "silhouetteCoefficient";
    private static final String METRICS_SUMMARY = "metricsSummary";
    private static final String EVAL_RESULT = "cluster_eval_result";
    private static final String LABELS = "labels";
    private static final String PREDICTIONS = "predictions";
    private static final String VECTOR_SIZE = "vectorSize";
    private static final String MEAN_AND_SUM = "meanAndSum";
	private static final long serialVersionUID = -1334962642325725386L;

	public EvalClusterBatchOp() {
		super(null);
	}

	public EvalClusterBatchOp(Params params) {
		super(params);
	}

	@Override
	public ClusterMetrics createMetrics(List <Row> rows) {
		return new ClusterMetrics(rows.get(0));
	}

	@Override
	public EvalClusterBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator in = checkAndGetFirst(inputs);
		String labelColName = this.getLabelCol();
		String predResultColName = this.getPredictionCol();
		String vectorColName = this.getVectorCol();
		DistanceType distanceType = getDistanceType();
		FastDistance distance = distanceType.getFastDistance();

		DataSet <Params> empty = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment().fromElements(
			new Params());
		DataSet <Params> labelMetrics = empty, vectorMetrics;

		if (null != labelColName) {
			DataSet <Row> data = in.select(new String[] {labelColName, predResultColName}).getDataSet();
			DataSet <Tuple1 <Map <Object, Integer>>> labels = data.flatMap(new FlatMapFunction <Row, Object>() {
				private static final long serialVersionUID = 6181506719667975996L;

				@Override
				public void flatMap(Row row, Collector <Object> collector) {
					if (EvaluationUtil.checkRowFieldNotNull(row)) {
						collector.collect(row.getField(0));
					}
				}
			}).reduceGroup(new EvaluationUtil.DistinctLabelIndexMap(false, null, null, false)).project(0);

			DataSet <Tuple1 <Map <Object, Integer>>> predictions = data.flatMap(new FlatMapFunction <Row, Object>() {
				private static final long serialVersionUID = 619373417169823128L;

				@Override
				public void flatMap(Row row, Collector <Object> collector) {
					if (EvaluationUtil.checkRowFieldNotNull(row)) {
						collector.collect(row.getField(1));
					}
				}
			}).reduceGroup(new EvaluationUtil.DistinctLabelIndexMap(false, null, null, false)).project(0);

			labelMetrics = data
				.rebalance()
				.mapPartition(new CalLocalPredResult())
				.withBroadcastSet(labels, LABELS)
				.withBroadcastSet(predictions, PREDICTIONS)
				.reduce(new ReduceFunction <LongMatrix>() {
					private static final long serialVersionUID = 3340266128816528106L;

					@Override
					public LongMatrix reduce(LongMatrix value1, LongMatrix value2) {
						value1.plusEqual(value2);
						return value1;
					}
				})
				.map(new RichMapFunction <LongMatrix, Params>() {
					private static final long serialVersionUID = -4218363116865487327L;

					@Override
                    public Params map(LongMatrix value) {
                        List<Tuple1<Map<Object, Integer>>> labels = getRuntimeContext().getBroadcastVariable(LABELS);
                        List<Tuple1<Map<Object, Integer>>> predictions = getRuntimeContext().getBroadcastVariable(PREDICTIONS);
                        return ClusterEvaluationUtil.extractParamsFromConfusionMatrix(value, labels.get(0).f0, predictions.get(0).f0);
                    }
                }).withBroadcastSet(labels, LABELS)
                .withBroadcastSet(predictions, PREDICTIONS);
        }
        if (null != vectorColName) {
            Tuple2 <DataSet <Tuple2<Vector, Row>>, DataSet <BaseVectorSummary>> statistics =
                StatisticsHelper.summaryHelper(inputs[0], null, vectorColName, new String[]{predResultColName});
            DataSet<Tuple2<Vector, String>> nonEmpty = statistics.f0
                .flatMap(new FilterEmptyRow())
                .withBroadcastSet(statistics.f1, VECTOR_SIZE);

            DataSet<Tuple3<String, DenseVector, DenseVector>> meanAndSum = nonEmpty
                .groupBy(1)
                .reduceGroup(new CalcMeanAndSum(distance))
                .withBroadcastSet(statistics.f1, VECTOR_SIZE);

            DataSet<BaseMetricsSummary> metricsSummary = nonEmpty
                .coGroup(meanAndSum)
                .where(1)
                .equalTo(0)
                .with(new CalcClusterMetricsSummary(distance))
                .withBroadcastSet(meanAndSum, MEAN_AND_SUM)
                .reduce(new EvaluationUtil.ReduceBaseMetrics());

            DataSet<Tuple1<Double>> silhouetteCoefficient = nonEmpty.map(
                new RichMapFunction<Tuple2<Vector, String>, Tuple1<Double>>() {
					private static final long serialVersionUID = 116926378586242272L;

					@Override
                    public Tuple1<Double> map(Tuple2<Vector, String> value) {
                        List<BaseMetricsSummary> list = getRuntimeContext().getBroadcastVariable(METRICS_SUMMARY);
                        return ClusterEvaluationUtil.calSilhouetteCoefficient(value,
                            (ClusterMetricsSummary)list.get(0));
                    }
                }).withBroadcastSet(metricsSummary, METRICS_SUMMARY)
                .aggregate(Aggregations.SUM, 0);

            vectorMetrics = metricsSummary.map(new ClusterEvaluationUtil.SaveDataAsParams()).withBroadcastSet(
                silhouetteCoefficient, SILHOUETTE_COEFFICIENT);
        } else {
            vectorMetrics = in.select(predResultColName)
                .getDataSet()
                .reduceGroup(new BasicClusterParams());
        }

        DataSet<Row> out = labelMetrics
            .union(vectorMetrics)
            .reduceGroup(new GroupReduceFunction<Params, Row>() {
				private static final long serialVersionUID = -4726713311986089251L;

				@Override
                public void reduce(Iterable<Params> values, Collector<Row> out) {
                    Params params = new Params();
                    for (Params p : values) {
                        params.merge(p);
                    }
                    out.collect(Row.of(params.toJson()));
                }
            });

        this.setOutputTable(DataSetConversionUtil.toTable(getMLEnvironmentId(),
            out, new TableSchema(new String[] {EVAL_RESULT}, new TypeInformation[] {Types.STRING})
        ));
        return this;
    }

    private static class FilterEmptyRow extends RichFlatMapFunction<Tuple2<Vector, Row>, Tuple2<Vector, String>> {
		private static final long serialVersionUID = 4239894668365119029L;
		private int vectorSize;
        private boolean isSparse;

	    @Override
        public void open(Configuration config){
            BaseVectorSummary summary = (BaseVectorSummary)(getRuntimeContext().getBroadcastVariable(VECTOR_SIZE).get(0));
            vectorSize = summary.vectorSize();
            isSparse = summary instanceof SparseVectorSummary;
        }

        @Override
        public void flatMap(Tuple2<Vector, Row> value, Collector<Tuple2<Vector, String>> out)
                        throws Exception {
            if(value.f0 != null && value.f1.getField(0) != null){
                if(isSparse){
                    ((SparseVector)value.f0).setSize(vectorSize);
                }
                out.collect(Tuple2.of(value.f0, value.f1.getField(0).toString()));
            }
        }
    }

    public static class CalcClusterMetricsSummary extends
        RichCoGroupFunction<Tuple2<Vector, String>, Tuple3<String, DenseVector, DenseVector>, BaseMetricsSummary> {
		private static final long serialVersionUID = 346446456425064132L;
		private FastDistance distance;

		public CalcClusterMetricsSummary(FastDistance distance) {
			this.distance = distance;
		}

        @Override
        public void coGroup(Iterable<Tuple2<Vector, String>> rows,
                            Iterable<Tuple3<String, DenseVector, DenseVector>> iterable,
                            Collector<BaseMetricsSummary> collector) {
            Tuple3<String, DenseVector, DenseVector> t = null;
            for(Tuple3<String, DenseVector, DenseVector> tuple : iterable){
                t = tuple;
            }
            collector.collect(ClusterEvaluationUtil.getClusterStatistics(rows, distance, t));
        }
    }

    public static class CalcMeanAndSum extends RichGroupReduceFunction<Tuple2<Vector, String>, Tuple3<String, DenseVector, DenseVector>> {
        private static final long serialVersionUID = 346446456425064132L;
        private int vectorSize;
        private FastDistance distance;

        public CalcMeanAndSum(FastDistance distance) {
            this.distance = distance;
        }

        @Override
        public void open(Configuration parameters){
            BaseVectorSummary summary = (BaseVectorSummary)(getRuntimeContext().getBroadcastVariable(VECTOR_SIZE).get(0));
            vectorSize = summary.vectorSize();
        }
        @Override
        public void reduce(Iterable<Tuple2<Vector, String>> rows, Collector<Tuple3<String, DenseVector, DenseVector>> collector) {
            collector.collect(ClusterEvaluationUtil.calMeanAndSum(rows, vectorSize, distance));
        }
    }

	public static class BasicClusterParams implements GroupReduceFunction <Row, Params> {
		private static final long serialVersionUID = 6863171040793536672L;

		@Override
		public void reduce(Iterable <Row> rows, Collector <Params> collector) {
			collector.collect(ClusterEvaluationUtil.getBasicClusterStatistics(rows));
		}
	}

	static class CalLocalPredResult extends RichMapPartitionFunction <Row, LongMatrix> {
		private static final long serialVersionUID = -3838344564725765751L;
		private Map <Object, Integer> labels;
		private Map <Object, Integer> predictions;

		@Override
		public void open(Configuration parameters) throws Exception {
			List <Tuple1 <Map <Object, Integer>>> list = getRuntimeContext().getBroadcastVariable(LABELS);
			this.labels = list.get(0).f0;
			list = getRuntimeContext().getBroadcastVariable(PREDICTIONS);
			this.predictions = list.get(0).f0;
		}

		@Override
		public void mapPartition(Iterable <Row> rows, Collector <LongMatrix> collector) {
			long[][] matrix = new long[predictions.size()][labels.size()];
			for (Row r : rows) {
				if (EvaluationUtil.checkRowFieldNotNull(r)) {
					int label = labels.get(r.getField(0));
					int pred = predictions.get(r.getField(1));
					matrix[pred][label] += 1;
				}
			}
			collector.collect(new LongMatrix(matrix));
		}
	}
}
