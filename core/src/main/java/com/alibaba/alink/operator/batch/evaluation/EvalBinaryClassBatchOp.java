package com.alibaba.alink.operator.batch.evaluation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.evaluation.BaseMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.EvaluationMetricsCollector;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.params.evaluation.EvalBinaryClassParams;
import com.alibaba.alink.params.evaluation.EvalMultiClassParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.calcSampleStatistics;
import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.checkRowFieldNotNull;
import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.extractLabelProbMap;

/**
 * Calculate the evaluation metrics for binary classifiction.
 * <p>
 * You can either give label column and predResult column or give label column and predDetail column. Once predDetail
 * column is given, the predResult column is ignored.
 * <p>
 * PositiveValue is optional, if given, it will be placed at the first position in the output label Array. If not given,
 * the labels are sorted in descending order.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.EVAL_METRICS))
@ParamSelectColumnSpec(name = "labelCol")
@ParamSelectColumnSpec(name = "predictionDetailCol", allowedTypeCollections = TypeCollections.STRING_TYPE)
@NameCn("二分类评估")
public class EvalBinaryClassBatchOp extends BatchOperator <EvalBinaryClassBatchOp> implements
	EvalBinaryClassParams <EvalBinaryClassBatchOp>,
	EvaluationMetricsCollector <BinaryClassMetrics, EvalBinaryClassBatchOp> {

	private static final long serialVersionUID = 5413408734356661786L;
	private static final Logger LOG = LoggerFactory.getLogger(EvalBinaryClassBatchOp.class);

	public EvalBinaryClassBatchOp() {
		this(null);
	}

	public EvalBinaryClassBatchOp(Params params) {
		super(params);
	}

	private static DataSet <Tuple2 <Map <Object, Integer>, Object[]>> calcLabels(DataSet <Row> data,
																				 final String positiveValue,
																				 TypeInformation <?> labelType) {
		return data.flatMap(new FlatMapFunction <Row, Object>() {
			private static final long serialVersionUID = 7858786264569432008L;

			@Override
			public void flatMap(Row row, Collector <Object> collector) {
				TreeMap <Object, Double> labelProbMap;
				if (checkRowFieldNotNull(row)) {
					labelProbMap = extractLabelProbMap(row, labelType);
					labelProbMap.keySet().forEach(collector::collect);
					collector.collect(row.getField(0));
				}
			}
		}).reduceGroup(new EvaluationUtil.DistinctLabelIndexMap(true, positiveValue, labelType, true));
	}

	static DataSet <BaseMetricsSummary> calLabelPredDetailLocal(
		DataSet <Tuple2 <Map <Object, Integer>, Object[]>> labels,
		DataSet <Tuple3 <Double, Boolean, Double>> sampleStatistics) {
		return ClassificationEvaluationUtil.calLabelPredDetailLocal(labels, sampleStatistics,
			labels.getExecutionEnvironment().fromElements(0.5));
	}

	@Override
	public BinaryClassMetrics createMetrics(List <Row> rows) {
		return new BinaryClassMetrics(rows.get(0));
	}

	@Override
	public EvalBinaryClassBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String labelColName = get(EvalMultiClassParams.LABEL_COL);
		TypeInformation <?> labelType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), labelColName);
		String positiveValue = get(EvalBinaryClassParams.POS_LABEL_VAL_STR);

		AkPreconditions.checkArgument(getParams().contains(EvalBinaryClassParams.PREDICTION_DETAIL_COL),
			new AkIllegalOperatorParameterException("Binary Evaluation must give predictionDetailCol!"));

		String predDetailColName = get(EvalMultiClassParams.PREDICTION_DETAIL_COL);
		TableUtil.assertSelectedColExist(in.getColNames(), labelColName, predDetailColName);

		DataSet <Row> data = in.select(new String[] {labelColName, predDetailColName}).getDataSet();

		DataSet <Tuple2 <Map <Object, Integer>, Object[]>> labels = calcLabels(data, positiveValue, labelType);
		DataSet <Tuple3 <Double, Boolean, Double>> sampleStatistics = calcSampleStatistics(data, labels, labelType);
		DataSet <BaseMetricsSummary> res = calLabelPredDetailLocal(labels, sampleStatistics);
		DataSet <BaseMetricsSummary> metrics = res.reduce(new EvaluationUtil.ReduceBaseMetrics());

		setOutput(metrics.flatMap(new EvaluationUtil.SaveDataAsParams()),
			new String[] {"Data"}, new TypeInformation[] {Types.STRING});

		return this;
	}

}
