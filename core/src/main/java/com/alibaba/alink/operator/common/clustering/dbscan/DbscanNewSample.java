package com.alibaba.alink.operator.common.clustering.dbscan;

import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import scala.util.hashing.MurmurHash3;

import java.io.Serializable;

import static com.alibaba.alink.operator.common.clustering.dbscan.Dbscan.UNCLASSIFIED;

public class DbscanNewSample implements Serializable {

	private static final long serialVersionUID = 6828060936541551678L;
	protected FastDistanceVectorData vec;
	protected long clusterId;
	protected Type type;
	protected int groupHashKey;

	public DbscanNewSample(FastDistanceVectorData vec, String[] groupColName) {
		this.vec = vec;
		this.clusterId = UNCLASSIFIED;
		this.type = null;
		this.groupHashKey = new MurmurHash3().arrayHash(groupColName, 0);
	}

	public int getGroupHashKey() {
		return groupHashKey;
	}

	public FastDistanceVectorData getVec() {
		return vec;
	}

	public long getClusterId() {
		return clusterId;
	}

	public void setClusterId(long clusterId) {
		this.clusterId = clusterId;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

}