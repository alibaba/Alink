package com.alibaba.alink.operator.common.clustering.common;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.AlinkSerializable;

import java.util.ArrayList;
import java.util.List;

/**
 * @author guotao.gt
 */
public class Cluster implements AlinkSerializable {
	protected long clusterId = -1;
	protected List <Sample> samples = new ArrayList <>();

	public Cluster() {
	}

	/**
	 * add
	 *
	 * @param sample
	 */
	public void addSample(Sample sample) {
		this.samples.add(sample);
		this.clusterId = sample.clusterId;
	}

	/**
	 * calc the center vector of a cluster
	 *
	 * @return
	 */
	public DenseVector mean() {
		if (null != samples && samples.size() > 0) {
			int dim = samples.get(0).getVector().size();
			DenseVector r = new DenseVector(dim);
			for (Sample dp : samples) {
				r.plusEqual(dp.getVector());
			}
			r.scaleEqual(1.0 / samples.size());
			return r;
		}
		return null;
	}

	/**
	 * get the center of a cluster
	 *
	 * @return
	 */
	public Center getCenter() {
		DenseVector denseVector = this.mean();
		long count = this.samples.size();
		return new Center(this.clusterId, count, denseVector);
	}

}
