package com.alibaba.alink.operator.common.clustering.common;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.AlinkSerializable;

/**
 * @author guotao.gt
 */
public class Center implements AlinkSerializable {

	public void setClusterId(long clusterId) {
		this.clusterId = clusterId;
	}

	/**
	 * which cluster the cluster belong to
	 */
	protected long clusterId = -1;

	/**
	 * how many sample belong to the cluster
	 */
	protected long count;

	/**
	 * the vector value of the sample
	 */
	protected DenseVector vector;

	public Center(long clusterId, long count, DenseVector vector) {
		this.clusterId = clusterId;
		this.count = count;
		this.vector = vector;
	}

	public long getClusterId() {
		return clusterId;
	}

	public long getCount() {
		return count;
	}

	public DenseVector getVector() {
		return vector;
	}
}
