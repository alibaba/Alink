package com.alibaba.alink.operator.local.evaluation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.ClusterEvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.ClusterMetrics;
import com.alibaba.alink.operator.common.evaluation.ClusterMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.LongMatrix;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummarizer;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.operator.common.statistics.basicstatistic.DenseVectorSummarizer;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.evaluation.EvalClusterParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Calculate the cluster evaluation metrics for clustering.
 * <p>
 * PredictionCol is required for evaluation. LabelCol is optional, if given, NMI/Purity/RI/ARI will be calcuated.
 * VectorCol is also optional, if given, SilhouetteCoefficient/SSB/SSW/Compactness/SP/DB /VRC will be calculated. If
 * only predictionCol is given, only K/ClusterArray/CountArray will be calculated.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.EVAL_METRICS))
@ParamSelectColumnSpec(name = "labelCol")
@ParamSelectColumnSpec(name = "predictionCol")
@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("聚类评估")
public final class EvalClusterLocalOp extends LocalOperator <EvalClusterLocalOp>
	implements EvalClusterParams <EvalClusterLocalOp>, EvaluationMetricsCollector <ClusterMetrics, EvalClusterLocalOp> {
	public static final String SILHOUETTE_COEFFICIENT = "silhouetteCoefficient";
	private static final String METRICS_SUMMARY = "metricsSummary";
	private static final String EVAL_RESULT = "cluster_eval_result";
	private static final String LABELS = "labels";
	private static final String VECTOR_SIZE = "vectorSize";
	private static final String MEAN_AND_SUM = "meanAndSum";
	private static final long serialVersionUID = -1334962642325725386L;

	public EvalClusterLocalOp() {
		super(null);
	}

	public EvalClusterLocalOp(Params params) {
		super(params);
	}

	@Override
	public ClusterMetrics createMetrics(List <Row> rows) {
		return new ClusterMetrics(rows.get(0));
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);

		if (0 == in.getOutputTable().getRows().size()) {
			ClusterMetrics metrics = ClusterMetricsSummary.createForEmptyDataset();
			setOutputTable(new MTable(new Row[] {Row.of(metrics.getParams().toJson())},
				new TableSchema(new String[] {EVAL_RESULT}, new TypeInformation[] {Types.STRING})));
			return ;
		}

		String labelColName = this.getLabelCol();
		String predResultColName = this.getPredictionCol();
		String vectorColName = this.getVectorCol();
		DistanceType distanceType = getDistanceType();
		FastDistance distance = distanceType.getFastDistance();

		Params labelMetrics = new Params();
		Params vectorMetrics = new Params();

		if (null != labelColName) {
			MTable data = in.select(new String[] {labelColName, predResultColName}).getOutputTable();

			HashSet <Object> labelsSet = new HashSet <>();
			for (Row row : data.getRows()) {
				if (EvaluationUtil.checkRowFieldNotNull(row)) {
					labelsSet.add(row.getField(0));
				}
			}
			Map <Object, Integer> labels = ClassificationEvaluationUtil
				.buildLabelIndexLabelArray(labelsSet, false, null, null, false).f0;

			HashSet <Object> predictionsSet = new HashSet <>();
			for (Row row : data.getRows()) {
				if (EvaluationUtil.checkRowFieldNotNull(row)) {
					predictionsSet.add(row.getField(1));
				}
			}
			Map <Object, Integer> predictions = ClassificationEvaluationUtil
				.buildLabelIndexLabelArray(predictionsSet, false, null, null, false).f0;

			long[][] matrix = new long[predictions.size()][labels.size()];
			for (Row r : data.getRows()) {
				if (EvaluationUtil.checkRowFieldNotNull(r)) {
					int label = labels.get(r.getField(0));
					int pred = predictions.get(r.getField(1));
					matrix[pred][label] += 1;
				}
			}
			LongMatrix lm = new LongMatrix(matrix);
			labelMetrics = ClusterEvaluationUtil.extractParamsFromConfusionMatrix(lm, labels, predictions);
		}
		if (null != vectorColName) {
			MTable data = in.select(new String[] {vectorColName, predResultColName}).getOutputTable();
			List <Tuple2 <Vector, String>> rows = data.getRows().stream()
				.map(d -> Tuple2.of(VectorUtil.getVector(d.getField(0)), d.getField(1).toString()))
				.collect(Collectors.toList());

			BaseVectorSummarizer summarizer = new DenseVectorSummarizer(false);
			for (Tuple2 <Vector, String> row : rows) {
				summarizer = summarizer.visit(row.getField(0));
			}
			BaseVectorSummary vectorSummary = summarizer.toSummary();
			int vectorSize = vectorSummary.vectorSize();

			Map <String, List <Tuple2 <Vector, String>>> grouped = new HashMap <>();
			for (Tuple2 <Vector, String> row : rows) {
				String prediction = row.getField(1);
				if (!grouped.containsKey(prediction)) {
					grouped.put(prediction, new ArrayList <>());
				}
				grouped.get(prediction).add(Tuple2.of(row.getField(0), prediction));
			}

			Map <Object, Tuple3 <String, DenseVector, DenseVector>> key2MeanAndSum = grouped.entrySet()
				.stream()
				.map(d ->
					Tuple2.of(d.getKey(), ClusterEvaluationUtil.calMeanAndSum(d.getValue(), vectorSize, distance)))
				.collect(Collectors.toMap(d -> d.f0, d -> d.f1));

			List <ClusterMetricsSummary> summaries = grouped.entrySet().stream()
				.map(d ->
					ClusterEvaluationUtil.getClusterStatistics(d.getValue(), distance, key2MeanAndSum.get(d.getKey())))
				.collect(Collectors.toList());
			ClusterMetricsSummary totalSummary = summaries.get(0);
			for (int i = 1; i < summaries.size(); i += 1) {
				totalSummary.merge(summaries.get(i));
			}

			double silhouetteCoefficient = 0.;
			for (Tuple2 <Vector, String> row : rows) {
				silhouetteCoefficient += ClusterEvaluationUtil.calSilhouetteCoefficient(row, totalSummary).f0;
			}
			vectorMetrics = totalSummary.toMetrics().getParams();
			final String SILHOUETTE_COEFFICIENT = "silhouetteCoefficient";
			vectorMetrics.set(SILHOUETTE_COEFFICIENT, silhouetteCoefficient / totalSummary.toMetrics().getCount());
		} else {
			MTable data = in.select(predResultColName).getOutputTable();
			vectorMetrics = ClusterEvaluationUtil.getBasicClusterStatistics(data.getRows());
		}
		Params out = labelMetrics.clone();
		out.merge(vectorMetrics);
		setOutputTable(new MTable(new Row[] {Row.of(out.toJson())},
			new TableSchema(new String[] {EVAL_RESULT}, new TypeInformation[] {Types.STRING})));
	}

	@Override
	public ClusterMetrics collectMetrics() {
		return EvaluationMetricsCollector.super.collectMetrics();
	}
}
