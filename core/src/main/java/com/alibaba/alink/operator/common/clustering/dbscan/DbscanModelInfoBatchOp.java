package com.alibaba.alink.operator.common.clustering.dbscan;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.utils.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DbscanModelInfoBatchOp
	extends ExtractModelInfoBatchOp <DbscanModelInfoBatchOp.DbscanModelInfo, DbscanModelInfoBatchOp> {
	private static final long serialVersionUID = 1735133462550836751L;

	public DbscanModelInfoBatchOp() {
		this(null);
	}

	public DbscanModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	public DbscanModelInfo createModelInfo(List <Row> rows) {
		return new DbscanModelInfo(rows);
	}

	public static class DbscanModelInfo implements Serializable {
		private static final long serialVersionUID = 5349212648420863302L;
		private Map <Integer, List <Object>> core;
		private Map <Integer, List <Object>> linked;
		private List <Object> noise;

		private int totalSamples;

		public DbscanModelInfo(List <Row> rows) {
			core = new HashMap <>();
			linked = new HashMap <>();
			noise = new ArrayList <>();
			totalSamples = 0;
			for (Row row : rows) {
				totalSamples++;
				Type type = Type.valueOf((String) row.getField(1));
				Object id = row.getField(0);
				long clusterId = (long) row.getField(2);
				switch (type) {
					case CORE: {
						List <Object> list = core.get((int) clusterId);
						if (null == list) {
							list = new ArrayList <>();
						}
						list.add(id);
						core.put((int) clusterId, list);
						break;
					}
					case LINKED: {
						List <Object> list = linked.get((int) clusterId);
						if (null == list) {
							list = new ArrayList <>();
						}
						list.add(id);
						linked.put((int) clusterId, list);
						break;
					}
					case NOISE: {
						noise.add(id);
						break;
					}
				}
			}
			for (int i = 0; i < getClusterNumber(); i++) {
				if (!linked.containsKey(i)) {
					linked.put(i, new ArrayList <>());
				}
			}
		}

		public int getClusterNumber() {
			return core.size();
		}

		public Object[] getCorePoints(int clusterId) {
			return core.get(clusterId).toArray(new Object[0]);
		}

		public Object[] getLinkedPoints(int clusterId) {
			return linked.get(clusterId).toArray(new Object[0]);
		}

		@Override
		public String toString() {
			StringBuilder sbd = new StringBuilder(PrettyDisplayUtils.displayHeadline("DbscanModelInfo", '-'));
			sbd.append("Dbcan clustering with ")
				.append(getClusterNumber())
				.append(" clusters on ")
				.append(totalSamples)
				.append(" samples.\n")
				.append(PrettyDisplayUtils.displayHeadline("CorePoints", '='))
				.append(PrettyDisplayUtils.displayMap(core, 3, true))
				.append("\n")
				.append(PrettyDisplayUtils.displayHeadline("LinkedPoints", '='))
				.append(PrettyDisplayUtils.displayMap(linked, 3, true))
				.append("\n")
				.append(PrettyDisplayUtils.displayHeadline("NoisePoints", '='))
				.append(PrettyDisplayUtils.displayList(noise, 3, false))
				.append("\n");
			return sbd.toString();
		}
	}
}
