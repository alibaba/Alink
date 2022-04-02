package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.feature.pca.PcaModelData;
import com.alibaba.alink.operator.common.feature.pca.PcaModelDataConverter;
import com.alibaba.alink.params.feature.PcaPredictParams;

import java.util.List;
import java.util.Map;

public class PcaModelDetector extends ModelOutlierDetector {

	private PcaModelData model = null;
	private final double threshold;
	private int[] featureIdxs = null;
	private boolean isVector;

	public PcaModelDetector(TableSchema modelSchema,
							TableSchema dataSchema,
							Params params) {
		super(modelSchema, dataSchema, params);
		threshold = 10;
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		model = new PcaModelDataConverter().load(modelRows);
		String[] colNames = ioSchema.f0;
		String[] featureColNames = model.featureColNames;
		String vectorColName = model.vectorColName;

		if (params.contains(PcaPredictParams.VECTOR_COL)) {
			vectorColName = params.get(PcaPredictParams.VECTOR_COL);
		}
		if (vectorColName != null) {
			this.isVector = true;
		}

		this.featureIdxs = checkGetColIdxs(isVector, featureColNames, vectorColName, colNames);
	}

	public static int[] checkGetColIdxs(Boolean isVector, String[] featureColNames, String vectorColName,
										String[] colNames) {
		int[] featureIdxs = null;
		int featureLength = 1;
		if (!isVector) {
			featureLength = featureColNames.length;
			featureIdxs = new int[featureLength];
			for (int i = 0; i < featureLength; ++i) {
				featureIdxs[i] = TableUtil.findColIndexWithAssert(colNames, featureColNames[i]);
			}
		} else {
			featureIdxs = new int[1];
			featureIdxs[0] = TableUtil.findColIndexWithAssert(colNames, vectorColName);
		}
		return featureIdxs;
	}

	@Override
	protected Tuple3 <Boolean, Double, Map <String, String>> detect(SlicedSelectedSample selection) throws Exception {
		double[] data = new double[this.model.nx];
		if (isVector) {
			Vector vector = VectorUtil.getVector(selection.get(featureIdxs[0]));
			if (vector instanceof SparseVector) {
				SparseVector feature = (SparseVector) vector;
				for (int i = 0; i < feature.getIndices().length; i++) {
					data[feature.getIndices()[i]] = feature.getValues()[i];
				}
			} else {
				Vector feature = (DenseVector) vector;
				for (int i = 0; i < feature.size(); i++) {
					data[i] = feature.get(i);
				}
			}

		} else {
			for (int i = 0; i < this.featureIdxs.length; ++i) {
				data[i] = ((Number) selection.get(this.featureIdxs[i])).doubleValue();
			}
		}

		double[] dataNe;
		if (model.idxNonEqual.length != data.length) {
			Integer[] indices = model.idxNonEqual;
			dataNe = new double[indices.length];
			for (int i = 0; i < indices.length; i++) {
				if (Math.abs(model.stddevs[i]) > 1e-12) {
					int idx = indices[i];
					dataNe[i] = (data[idx] - model.means[i]) / model.stddevs[i];
				}
			}
		} else {
			for (int i = 0; i < data.length; i++) {
				if (Math.abs(model.stddevs[i]) > 1e-12) {
					data[i] = (data[i] - model.means[i]) / model.stddevs[i];
				}
			}
			dataNe = data;
		}

		//predict the score.
		double score = 0.;
		for (int k = 0; k < model.p; ++k) {
			double localScore = 0.;
			for (int i = 0; i < dataNe.length; ++i) {
				localScore += dataNe[i] * model.coef[k][i];
			}
			double temp = Math.pow(localScore, 2);
			score += temp / model.lambda[k];
		}

		//result.set(0, new DenseVector(new double[] {score}));

		double prob = 0;
		return Tuple3.of(prob > threshold, prob, null);
	}
}
