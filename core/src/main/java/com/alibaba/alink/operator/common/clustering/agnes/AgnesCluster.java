package com.alibaba.alink.operator.common.clustering.agnes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author guotao.gt
 */
public class AgnesCluster implements Serializable {
	private static final long serialVersionUID = -3460634266927981428L;
	private int clusterId;

	private AgnesSample firstSample;
	private List <AgnesSample> agnesSamples = new ArrayList <>();

	public AgnesCluster(int clusterId, AgnesSample agnesSample) {
		this.agnesSamples.add(agnesSample);
		this.clusterId = clusterId;
		this.firstSample = agnesSample;
	}

	public AgnesSample getFirstSample() {
		return firstSample;
	}

	public List <AgnesSample> getAgnesSamples() {
		return agnesSamples;
	}

	public void addDataPoints(AgnesSample agnesSample) {
		this.agnesSamples.add(agnesSample);
	}

	public int getClusterId() {
		return clusterId;
	}
}