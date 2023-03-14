package com.alibaba.alink.operator.common.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.JsonConverter;

import java.io.Serializable;
import java.util.List;

public class FeatureBundles implements Serializable {
	int dimVector;
	int numFeatures;
	String schemaStr;
	int[] toEfbIndex;
	boolean[] isNumeric;

	public int getDimVector() {
		return dimVector;
	}

	public void setDimVector(int dimVector) {
		this.dimVector = dimVector;
	}

	public int getNumFeatures() {
		return numFeatures;
	}

	public void setNumFeatures(int numFeatures) {
		this.numFeatures = numFeatures;
	}

	public String getSchemaStr() {
		return schemaStr;
	}

	public void setSchemaStr(String schemaStr) {
		this.schemaStr = schemaStr;
	}

	public int[] getToEfbIndex() {
		return toEfbIndex;
	}

	public void setToEfbIndex(int[] toEfbIndex) {
		this.toEfbIndex = toEfbIndex;
	}

	public boolean[] getIsNumeric() {
		return isNumeric;
	}

	public void setIsNumeric(boolean[] isNumeric) {
		this.isNumeric = isNumeric;
	}

	public FeatureBundles() {}

	public FeatureBundles(int dim, List <int[]> bundles) {
		this.dimVector = dim;
		this.numFeatures = bundles.size();
		this.toEfbIndex = new int[dimVector];
		this.isNumeric = new boolean[numFeatures];
		StringBuilder sbd = new StringBuilder();
		for (int k = 0; k < bundles.size(); k++) {
			boolean isNumericCol = (bundles.get(k).length <= 1);
			sbd.append((k > 0) ? "," : "")
				.append("efb_").append(k).append(" ")
				.append(isNumericCol ? "double" : "string");
			isNumeric[k] = isNumericCol;
		}
		this.schemaStr = sbd.toString();
		for (int k = 0; k < bundles.size(); k++) {
			for (int idx : bundles.get(k)) {
				toEfbIndex[idx] = k;
			}
		}
	}

	public Row map(Object obj) {
		Row row = new Row(this.numFeatures);
		SparseVector vec = VectorUtil.getSparseVector(obj);
		int[] indices = vec.getIndices();
		double[] values = vec.getValues();
		for (int i = 0; i < indices.length; i++) {
			if (indices[i] < this.dimVector) {
				int efbIndex = toEfbIndex[indices[i]];
				if (isNumeric[efbIndex]) {
					row.setField(efbIndex, values[i]);
				} else {
					row.setField(efbIndex, String.valueOf(indices[i]));
				}
			}
		}
		return row;
	}

	public String toJson() {
		return JsonConverter.toJson(this);
	}

	public static FeatureBundles fromJson(String json) {
		return JsonConverter.fromJson(json, FeatureBundles.class);
	}
}
