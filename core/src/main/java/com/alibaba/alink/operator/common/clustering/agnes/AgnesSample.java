package com.alibaba.alink.operator.common.clustering.agnes;

import com.alibaba.alink.common.linalg.DenseVector;

import java.io.Serializable;

/**
 * @author guotao.gt
 */
public class AgnesSample implements Serializable {
	private static final long serialVersionUID = -7913806684961377173L;

	private String parentId;

	private Long mergeIter;
	private String sampleId;
	private long clusterId;
	private double weight = 1.0;
	private DenseVector vector;

	public AgnesSample() {
	}

	public AgnesSample(String sampleId, long clusterId, DenseVector vector, double weight) {
		this.sampleId = sampleId;
		this.clusterId = clusterId;
		this.vector = vector;
		this.weight = weight;
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

	public void setParentId(String parentId) {
		this.parentId = parentId;
	}

	public String getParentId() {
		return parentId;
	}

	public Long getMergeIter() {
		return mergeIter;
	}

	public void setMergeIter(int mergeIter) {
		this.mergeIter = (long) mergeIter;
	}

	public String getSampleId() {
		return sampleId;
	}

	public double getWeight() {
		return weight;
	}
}
