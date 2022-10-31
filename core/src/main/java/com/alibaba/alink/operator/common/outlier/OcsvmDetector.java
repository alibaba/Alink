package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.common.outlier.OcsvmModelData.SvmModelData;
import com.alibaba.alink.params.outlier.OcsvmDetectorParams;

import java.util.Arrays;
import java.util.Map;

import static com.alibaba.alink.operator.common.outlier.OcsvmKernel.svmPredict;
import static com.alibaba.alink.operator.common.outlier.OcsvmKernel.svmTrain;

public class OcsvmDetector extends OutlierDetector {

	public OcsvmDetector(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	@Override
	protected Tuple3 <Boolean, Double, Map <String, String>>[] detect(MTable series, boolean detectLast) {
		String vecCol = params.get(OcsvmDetectorParams.VECTOR_COL);
		series = vecCol != null ? series.select(vecCol)
			: series.select(params.get(OcsvmDetectorParams.FEATURE_COLS));
		OcsvmTrain ocsvmTrain = new OcsvmTrain(params);
		OcsvmPredict ocsvmPredict = new OcsvmPredict(params);

		ocsvmPredict.loadModel(ocsvmTrain.train(series));

		int iStart = detectLast ? series.getNumRow() - 1 : 0;

		Tuple3 <Boolean, Double, Map <String, String>>[] results = new Tuple3[series.getNumRow() - iStart];

		for (int i = iStart; i < series.getNumRow(); i++) {
			double score = ocsvmPredict.predict(series.getRow(i));
			results[i - iStart] = Tuple3.of(score > 0, score, null);
		}
		return results;
	}

	public final static class OcsvmTrain implements OcsvmDetectorParams <OcsvmTrain> {
		private final Params params;

		public OcsvmTrain() {
			params = new Params();
		}

		public OcsvmTrain(Params params) {
			this.params = params == null ? new Params() : params;
		}

		public OcsvmModelData train(MTable input) {
			Vector[] sample = OutlierUtil.getVectors(input, params);
			if (params.get(OcsvmDetectorParams.GAMMA) < 1.0e-18) {
				params.set(OcsvmDetectorParams.GAMMA, 1.0 / sample[0].size());
			}
			OcsvmModelData ocsvmModelData = new OcsvmModelData();
			ocsvmModelData.featureColNames = getFeatureCols();
			ocsvmModelData.kernelType = getKernelType();
			ocsvmModelData.coef0 = getCoef0();
			ocsvmModelData.degree = getDegree();
			ocsvmModelData.gamma = params.get(OcsvmDetectorParams.GAMMA);
			ocsvmModelData.vectorCol = getVectorCol();
			SvmModelData[] modelArray = new SvmModelData[1];
			modelArray[0] = svmTrain(sample, params);
			ocsvmModelData.models = modelArray;
			return ocsvmModelData;
		}

		@Override
		public Params getParams() {
			return params;
		}
	}

	public final static class OcsvmPredict implements OcsvmDetectorParams <OcsvmPredict> {
		private final Params params;
		private transient OcsvmModelData ocsvmModel;
		private String vectorColName;
		private double gamma;
		private double coef0;
		private int degree;
		private KernelType kernelType;

		public OcsvmPredict() {
			this(new Params());
		}

		public OcsvmPredict(Params params) {
			this.params = params == null ? new Params() : params;
		}

		public void loadModel(OcsvmModelData model) {
			this.ocsvmModel = model;
			vectorColName = model.vectorCol;
			gamma = model.gamma;
			kernelType = model.kernelType;
			degree = model.degree;
			coef0 = model.coef0;
		}

		public double predict(Row row) {
			Vector vec;
			if (vectorColName != null) {
				vec = VectorUtil.getVector(row.getField(0));

			} else {
				vec = new DenseVector(row.getArity());
				for (int i = 0; i < row.getArity(); ++i) {
					vec.set(i, ((Number) row.getField(i)).doubleValue());
				}
			}

			double score = 0.0;
			for (SvmModelData model : ocsvmModel.models) {
				double pred = svmPredict(model, vec, kernelType, gamma, coef0, degree);
				score -= pred;
			}
			return score;
		}

		@Override
		public Params getParams() {
			return params;
		}
	}
}
