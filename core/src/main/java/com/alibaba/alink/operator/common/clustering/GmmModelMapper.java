package com.alibaba.alink.operator.common.clustering;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.RichModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.statistics.basicstatistic.MultivariateGaussian;
import com.alibaba.alink.params.clustering.GmmPredictParams;

import java.util.List;

/**
 * Model mapper of Gaussian Mixture Model.
 */
public class GmmModelMapper extends RichModelMapper {

	private static final long serialVersionUID = -4999832537099548829L;
	private int vectorColIdx;
	private GmmModelData modelData;
	private MultivariateGaussian[] multivariateGaussians;
	private transient ThreadLocal <double[]> threadLocalProb;

	public GmmModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		String vectorColName = this.params.get(GmmPredictParams.VECTOR_COL);
		vectorColIdx = TableUtil.findColIndexWithAssertAndHint(dataSchema.getFieldNames(), vectorColName);
	}

	@Override
	protected Object predictResult(SlicedSelectedSample selection) throws Exception {
		return predictResultDetail(selection).f0;
	}

	@Override
	protected Tuple2 <Object, String> predictResultDetail(SlicedSelectedSample selection) throws Exception {
		Vector sample = VectorUtil.getVector(selection.get(vectorColIdx));
		double[] prob = threadLocalProb.get();
		int k = modelData.k;
		double probSum = 0.;
		for (int i = 0; i < k; i++) {
			double density = this.multivariateGaussians[i].pdf(sample);
			double p = modelData.data.get(i).weight * density;
			prob[i] = p;
			probSum += p;
		}
		for (int i = 0; i < k; i++) {
			prob[i] /= probSum;
		}

		int maxIndex = 0;
		double maxProb = prob[0];

		for (int i = 1; i < k; i++) {
			if (prob[i] > maxProb) {
				maxProb = prob[i];
				maxIndex = i;
			}
		}

		return Tuple2.of((long) maxIndex, VectorUtil.serialize(new DenseVector(prob)));
	}

	@Override
	protected TypeInformation <?> initPredResultColType(TableSchema modelSchema) {
		return Types.LONG;
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		this.modelData = new GmmModelDataConverter().load(modelRows);
		this.multivariateGaussians = new MultivariateGaussian[this.modelData.k];
		for (int i = 0; i < this.modelData.k; i++) {
			this.multivariateGaussians[i] = new MultivariateGaussian(modelData.data.get(i).mean,
				GmmModelData.expandCovarianceMatrix(modelData.data.get(i).cov, modelData.dim));
		}
		threadLocalProb = ThreadLocal.withInitial(() -> new double[this.modelData.k]);
	}
}
