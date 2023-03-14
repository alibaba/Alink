package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.outlier.OcsvmModelData.SvmModelData;
import com.alibaba.alink.params.outlier.HaskernelType.KernelType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.operator.common.outlier.OcsvmKernel.svmPredict;

public class OcsvmModelDetector extends ModelOutlierDetector {
	private static final long serialVersionUID = 6504098446269455446L;
	private int[] featureIdx;
	private int vectorIndex = -1;
	private OcsvmModelData modelData;
	private DenseVector localX;
	private double gamma;
	private double coef0;
	private int degree;
	private KernelType kernelType;

	public OcsvmModelDetector(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		modelData = new OcsvmModelDataConverter().load(modelRows);
		this.gamma = modelData.gamma;
		this.coef0 = modelData.coef0;
		this.degree = modelData.degree;
		this.kernelType = modelData.kernelType;
		if (modelData.featureColNames != null) {
			featureIdx = TableUtil.findColIndicesWithAssertAndHint(
				getSelectedCols(),
				modelData.featureColNames
			);
			localX = new DenseVector(featureIdx.length);
		}
		String vectorCol = modelData.vectorCol;
		if (vectorCol != null && !vectorCol.isEmpty()) {
			this.vectorIndex = TableUtil.findColIndexWithAssertAndHint(getSelectedCols(), vectorCol);
		}
	}

	@Override
	protected Tuple3 <Boolean, Double, Map <String, String>> detect(SlicedSelectedSample selection) {
		double score = 0.0;
		for (SvmModelData model : modelData.models) {
			double pred = predictSingle(selection, model);
			score -= pred;
		}
		boolean finalResult = score >= 0.0;
		Map <String, String> detail = new HashMap <>();
		detail.put("outlier_score", String.valueOf(score));
		return Tuple3.of(finalResult, score, detail);
	}

	public double predictSingle(SlicedSelectedSample selection, SvmModelData model) {
		Vector x;
		if (this.vectorIndex != -1) {
			Object obj = selection.get(this.vectorIndex);
			x = VectorUtil.getVector(obj);
			return svmPredict(model, x, kernelType, gamma, coef0, degree);
		} else {
			for (int i = 0; i < featureIdx.length; ++i) {
				localX.set(i, ((Number) selection.get(featureIdx[i])).doubleValue());
			}
			return svmPredict(model, localX, kernelType, gamma, coef0, degree);
		}
	}
}
