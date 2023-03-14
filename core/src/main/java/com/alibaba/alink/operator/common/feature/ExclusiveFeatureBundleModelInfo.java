package com.alibaba.alink.operator.common.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Summary of ExclusiveFeatureBundleModel.
 */
public class ExclusiveFeatureBundleModelInfo implements Serializable {
	public FeatureBundles bundles;
	public final ArrayList <Integer>[] bundleIndexes;

	public ExclusiveFeatureBundleModelInfo(List <Row> rows) {
		ExclusiveFeatureBundleModelDataConverter converter = new ExclusiveFeatureBundleModelDataConverter();
		bundles = converter.load(rows);
		bundleIndexes = new ArrayList[bundles.numFeatures];
		for (int k = 0; k < bundles.numFeatures; k++) {
			bundleIndexes[k] = new ArrayList <>();
		}
		for (int i = 0; i < bundles.toEfbIndex.length; i++) {
			bundleIndexes[bundles.toEfbIndex[i]].add(i);
		}
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder(
			PrettyDisplayUtils.displayHeadline("ExclusiveFeatureBundleModelInfo", '-'));
		sbd.append("Sparse Vector Dimension : ").append(bundles.dimVector)
			.append(" ,    Number of Feature Bundles : ").append(bundles.numFeatures)
			.append("\nFeature Schema String : ")
			.append(PrettyDisplayUtils.display(bundles.schemaStr))
			.append("\nFeature Bundles Info : ");
		for (int k = 0; k < bundles.numFeatures; k++) {
			sbd.append("\nBundle " + k + " : ")
				.append(PrettyDisplayUtils.displayList(bundleIndexes[k]));
		}
		sbd.append("\n");
		return sbd.toString();
	}

}
