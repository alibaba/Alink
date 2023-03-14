package com.alibaba.alink.operator.common.clustering.common;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.AlinkSerializable;

/**
 * @author guotao.gt
 */
public class Sample implements AlinkSerializable {

	/**
	 * the identity of a sample
	 */
	protected String sampleId;

	/**
	 * the vector value of the sample
	 */
	protected DenseVector vector;

	/**
	 * which cluster the cluster belong to
	 */
	protected long clusterId = -1;

	/**
	 * for group clustering,not necessary
	 */
	private String[] groupColNames;

	public Sample() {
	}

	public Sample(String sampleId, double[] vector) {
		this(sampleId, new DenseVector(vector));
	}

	public Sample(String sampleId, DenseVector vector) {
		this.sampleId = sampleId;
		this.vector = vector;
	}

	public Sample(String sampleId, DenseVector vector, long clusterId, String[] groupColNames) {
		this.sampleId = sampleId;
		this.vector = vector;
		this.clusterId = clusterId;
		this.groupColNames = groupColNames;
	}

	public DenseVector getVector() {
		return vector;
	}

	public long getClusterId() {
		return clusterId;
	}

	public void setClusterId(long clusterId) {
		this.clusterId = clusterId;
	}

	public String getSampleId() {
		return sampleId;
	}

	public String[] getGroupColNames() {
		return groupColNames;
	}

	public String getGroupColNamesString() {
		StringBuilder sb = new StringBuilder();
		for (String key : this.groupColNames) {
			sb.append(key).append("\001");
		}
		return sb.toString();
	}
}
