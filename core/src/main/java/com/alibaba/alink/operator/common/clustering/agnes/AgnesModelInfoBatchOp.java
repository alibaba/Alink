package com.alibaba.alink.operator.common.clustering.agnes;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.utils.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AgnesModelInfoBatchOp
	extends ExtractModelInfoBatchOp <AgnesModelInfoBatchOp.AgnesModelSummary, AgnesModelInfoBatchOp> {
	private static final long serialVersionUID = 1735133462550836751L;

	public AgnesModelInfoBatchOp() {
		this(null);
	}

	public AgnesModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	public AgnesModelSummary createModelInfo(List <Row> rows) {
		AgnesModelInfoBatchOp.AgnesModelSummary summary = new AgnesModelInfoBatchOp.AgnesModelSummary();
		summary.cluster = new HashMap <>();
		summary.totalSamples = 0;
		for (Row row : rows) {
			summary.totalSamples++;
			Object id = row.getField(0);
			int clusterId = ((Number) row.getField(1)).intValue();
			List <Object> list = summary.cluster.get(clusterId);
			if (null == list) {
				list = new ArrayList <>();
			}
			list.add(id);
			summary.cluster.put(clusterId, list);
		}
		return summary;
	}

	public static class AgnesModelSummary {
		private static final long serialVersionUID = 5349212648420863302L;
		private Map <Integer, List <Object>> cluster;

		private int totalSamples = 0;

		public AgnesModelSummary() {
		}

		public int getClusterNumber() {
			return cluster.size();
		}

		public Object[] getPoints(int clusterId) {
			return cluster.get(clusterId).toArray(new Object[0]);
		}

		@Override
		public String toString() {
			StringBuilder sbd = new StringBuilder(PrettyDisplayUtils.displayHeadline("AgnesModelSummary", '-'));
			sbd.append("Agnes clustering with ")
				.append(getClusterNumber())
				.append(" clusters on ")
				.append(totalSamples)
				.append(" samples.\n");
			sbd.append(PrettyDisplayUtils.displayHeadline("Clusters", '='));
			sbd.append(PrettyDisplayUtils.displayMap(cluster, 10, true)).append("\n");
			return sbd.toString();
		}
	}
}
