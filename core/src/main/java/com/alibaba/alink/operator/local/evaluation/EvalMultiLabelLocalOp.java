package com.alibaba.alink.operator.local.evaluation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.MultiClassMetrics;
import com.alibaba.alink.operator.common.evaluation.MultiLabelMetrics;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.evaluation.EvalMultiLabelParams;

import java.util.HashSet;
import java.util.List;

/**
 * Evaluation for multi-label classification task.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.EVAL_METRICS))
@NameCn("多标签分类评估")
public class EvalMultiLabelLocalOp extends LocalOperator <EvalMultiLabelLocalOp>
	implements EvalMultiLabelParams <EvalMultiLabelLocalOp>,
	EvaluationMetricsCollector <MultiLabelMetrics, EvalMultiLabelLocalOp> {

	public static String LABELS = "labels";

	public EvalMultiLabelLocalOp() {
		super(null);
	}

	public EvalMultiLabelLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator in = checkAndGetFirst(inputs);

		int indexLabel = TableUtil.findColIndex(in.getColNames(), this.getLabelCol());
		int indexPredict = TableUtil.findColIndex(in.getColNames(), this.getPredictionCol());
		AkPreconditions.checkArgument(indexLabel >= 0 && indexPredict >= 0,
			new AkIllegalOperatorParameterException("Can not find label column or prediction column!"));

		List <Row> dataSet
			= in.select(new String[] {this.getLabelCol(), this.getPredictionCol()}).getOutputTable().getRows();

		Tuple3 <Integer, Class, Integer> labelSizeClass
			= getLabelNumberAndMaxK(dataSet, getPredictionRankingInfo(), getPredictionRankingInfo());

		MultiLabelMetrics metrics = EvaluationUtil.getMultiLabelMetrics(
			in.getOutputTable().select(this.getLabelCol(), this.getPredictionCol()).getRows(),
			labelSizeClass, getLabelRankingInfo(), getPredictionRankingInfo()
		).toMetrics();

		this.setOutputTable(new MTable(
			new Row[] {metrics.serialize()},
			new TableSchema(new String[] {"multilabel_eval_result"}, new TypeInformation[] {Types.STRING})
		));
	}

	@Override
	public MultiLabelMetrics createMetrics(List <Row> rows) {
		return new MultiLabelMetrics(rows.get(0));
	}

	@Override
	public MultiLabelMetrics collectMetrics() {
		return EvaluationMetricsCollector.super.collectMetrics();
	}

	public static Tuple3 <HashSet <Object>, Class, Integer> subGetLabelNumberAndMaxK(
		Row value, String labelKObject, String predictionKObject) {
		HashSet <Object> hashSet = new HashSet <>();
		if (!EvaluationUtil.checkRowFieldNotNull(value)) {
			return Tuple3.of(hashSet, null, 0);
		}
		List <Object> labels = EvaluationUtil.extractDistinctLabel((String) value.getField(0),
			labelKObject);
		List <Object> predictions = EvaluationUtil.extractDistinctLabel((String) value.getField(1),
			predictionKObject);
		Class labelClass = null;
		Class predictionClass = null;
		Class outputClass;
		if (labels.size() > 0) {
			labelClass = labels.get(0).getClass();
		}
		if (predictions.size() > 0) {
			predictionClass = predictions.get(0).getClass();
		}
		if (labelClass == null) {
			outputClass = predictionClass;
			hashSet.addAll(predictions);
		} else if (predictionClass == null) {
			outputClass = labelClass;
			hashSet.addAll(labels);
		} else if (labelClass.equals(predictionClass)) {
			outputClass = labelClass;
			hashSet.addAll(labels);
			hashSet.addAll(predictions);
		} else {
			outputClass = String.class;
			for (Object object : labels) {
				hashSet.add(object.toString());
			}
			for (Object object : predictions) {
				hashSet.add(object.toString());
			}
		}
		return Tuple3.of(hashSet, outputClass, Math.max(labels.size(), predictions.size()));
	}

	/**
	 * Extract the label number and maxK.
	 *
	 * @param data              dataset
	 * @param labelKObject      the key for the label json
	 * @param predictionKObject the key for the prediction json
	 * @return LabelNumber, Label class, MaxK
	 */

	public static Tuple3 <Integer, Class, Integer> getLabelNumberAndMaxK(
		List <Row> data, String labelKObject, String predictionKObject) {
		Tuple3 <HashSet <Object>, Class, Integer> value1
			= subGetLabelNumberAndMaxK(data.get(0), labelKObject, predictionKObject);
		Tuple3 <HashSet <Object>, Class, Integer> value2 = null;
		for (int i = 1; i < data.size(); i++) {
			value2 = subGetLabelNumberAndMaxK(data.get(i), labelKObject, predictionKObject);
			if (value1.f1 == null) {
				AkPreconditions.checkArgument(value1.f0.size() == 0 && value1.f2 == 0,
					"LabelClass is null but label size is not 0!");
				value1 = value2;
			} else if (value2.f1 == null) {
				AkPreconditions.checkArgument(value2.f0.size() == 0 && value2.f2 == 0,
					"LabelClass is null but label size is not 0!");
			} else if (value1.f1.equals(value2.f1)) {
				value1.f0.addAll(value2.f0);
				value1.f2 = Math.max(value1.f2, value2.f2);
			} else {
				HashSet <Object> hashSet = new HashSet <>();
				for (Object object : value1.f0) {
					hashSet.add(object.toString());
				}
				for (Object object : value2.f0) {
					hashSet.add(object.toString());
				}
				value1 = Tuple3.of(hashSet, String.class, Math.max(value1.f2, value2.f2));
			}
		}
		AkPreconditions.checkState(value1.f0.size() > 0,
			"There is no valid data in the whole dataSet, please check the input for evaluation!");
		return Tuple3.of(value1.f0.size(), value1.f1, value1.f2);

	}

}
