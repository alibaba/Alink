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
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.evaluation.EvalBinaryClassParams;
import com.alibaba.alink.params.evaluation.EvalMultiClassParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
public class EvalBinaryClassLocalOp extends LocalOperator <EvalBinaryClassLocalOp> implements
	EvalBinaryClassParams <EvalBinaryClassLocalOp>,
	EvaluationMetricsCollector <BinaryClassMetrics, EvalBinaryClassLocalOp> {

	private static final Logger LOG = LoggerFactory.getLogger(EvalBinaryClassLocalOp.class);


	public EvalBinaryClassLocalOp() {
		this(null);
	}

	public EvalBinaryClassLocalOp(Params params) {
		super(params);
	}

	private static Tuple2 <Map <Object, Integer>, Object[]> calcLabels(
		List <Row> data, boolean binary, final String positiveValue, TypeInformation <?> labelType
	) {
		Set <Object> labels = EvaluationUtil.extractLabelProbMap(data.get(0), labelType).keySet();
		return ClassificationEvaluationUtil.buildLabelIndexLabelArray(
			labels, binary, positiveValue, labelType, true);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		String labelColName = get(EvalMultiClassParams.LABEL_COL);
		TypeInformation <?> labelType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), labelColName);
		String positiveValue = get(EvalBinaryClassParams.POS_LABEL_VAL_STR);

		AkPreconditions.checkArgument(getParams().contains(EvalBinaryClassParams.PREDICTION_DETAIL_COL),
			new AkIllegalOperatorParameterException("Binary Evaluation must give predictionDetailCol!"));

		String predDetailColName = get(EvalMultiClassParams.PREDICTION_DETAIL_COL);
		TableUtil.assertSelectedColExist(in.getColNames(), labelColName, predDetailColName);

		List <Row> data = in.select(new String[] {labelColName, predDetailColName}).getOutputTable().getRows();
		Tuple2 <Map <Object, Integer>, Object[]> labels = calcLabels(data, true, positiveValue, labelType);

		List <Tuple3 <Double, Boolean, Double>> sampleStatistics
			= ClassificationEvaluationUtil.calcSampleStatistics(data, labels, labelType);

		BinaryClassMetrics metrics = ClassificationEvaluationUtil
			.calLabelPredDetailLocal(labels, sampleStatistics, 0.5)
			.toMetrics();

		setOutputTable(new MTable(new Row[] {metrics.serialize()}, new TableSchema(
			new String[] {"binaryclass_eval_result"}, new TypeInformation[] {Types.STRING})));

	}

	@Override
	public BinaryClassMetrics createMetrics(List <Row> rows) {
		return new BinaryClassMetrics(rows.get(0));
	}

	@Override
	public BinaryClassMetrics collectMetrics() {
		return EvaluationMetricsCollector.super.collectMetrics();
	}
}
