package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.params.clustering.GmmPredictParams;
import com.alibaba.alink.params.clustering.GmmTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Gaussian Mixture is a kind of clustering algorithm.
 * <p>
 * Gaussian Mixture clustering performs expectation maximization for multivariate Gaussian
 * Mixture Models (GMMs).  A GMM represents a composite distribution of
 * independent Gaussian distributions with associated "mixing" weights
 * specifying each's contribution to the composite.
 * <p>
 * Given a set of sample points, this class will maximize the log-likelihood
 * for a mixture of k Gaussians, iterating until the log-likelihood changes by
 * less than convergenceTol, or until it has reached the max number of iterations.
 * While this process is generally guaranteed to converge, it is not guaranteed
 * to find a global optimum.
 */
@NameCn("高斯混合模型")
public class GaussianMixture extends Trainer <GaussianMixture, GaussianMixtureModel> implements
	GmmTrainParams <GaussianMixture>,
	GmmPredictParams <GaussianMixture>, HasLazyPrintModelInfo <GaussianMixture> {

	private static final long serialVersionUID = -5048838310729183267L;

	public GaussianMixture() {
		super();
	}

	public GaussianMixture(Params params) {
		super(params);
	}

}
