package com.alibaba.alink.common.fe.def.day;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.fe.def.statistics.BaseCategoricalStatistics;
import com.alibaba.alink.operator.batch.feature.GenerateFeatureOfLatestNDaysBatchOp;

import java.util.ArrayList;
import java.util.List;

public class NDaysCategoricalStatFeatures
	extends BaseDaysStatFeatures <NDaysCategoricalStatFeatures> {

	public BaseCategoricalStatistics[] types;
	public String[] featureCols;
	public String[] featureItems; //only for cate item.

	public NDaysCategoricalStatFeatures() {
		super();
	}

	public BaseCategoricalStatistics[] getCategoricalStatistics() {
		return types;
	}

	public NDaysCategoricalStatFeatures setCategoricalDaysStatistics(
		BaseCategoricalStatistics... categoricalStatistics) {
		this.types = categoricalStatistics;
		return this;
	}

	public String[] getFeatureCols() {
		return featureCols;
	}

	public NDaysCategoricalStatFeatures setFeatureCols(String... featureCols) {
		this.featureCols = featureCols;
		return this;
	}

	public String[] getFeatureItems() {
		return this.featureItems;
	}

	public NDaysCategoricalStatFeatures setFeatureItems(String... featureItems) {
		this.featureItems = featureItems;
		return this;
	}

	//type[__col(_items)]_days[__conditionCol_condition][__groups]
	public String[] getOutColNames() {
		int numCondition = this.conditions == null ? 1 : this.conditions.length;
		List <String> outColNames = new ArrayList <>();

		String deli = "__";
		for (BaseCategoricalStatistics type : this.types) {
			String[] outFeatureNames = this.featureCols;
			if (CategoricalDaysStatistics.CATES_CNT == type) {
				outFeatureNames = this.featureItems;
			}
			for (int ic = 0; ic < numCondition; ic++) {
				for (String featureItem : outFeatureNames) {
					for (String nDay : nDays) {
						StringBuilder sbd = new StringBuilder();
						sbd.append(type.name().toLowerCase());
						if (CategoricalDaysStatistics.TOTAL_COUNT != type) {
							sbd.append(deli)
								.append(featureItem);
						}
						sbd.append(deli)
							.append(nDay);
						if (this.conditions != null) {
							if(this.conditions[ic] != null && this.conditions[ic].length != 0) {
								sbd.append(deli)
									.append(this.conditionCol)
									.append(deli)
									.append(String.join("_", this.conditions[ic]));
							}
						}

						if (this.groupCols != null) {
							if (this.groupCols.length == 1 || !this.groupCols[0].equals(
								GenerateFeatureOfLatestNDaysBatchOp.DEFAULT_GROUP_COL)) {
								sbd.append(deli)
									.append(String.join("_", groupCols));
							}
						}

						outColNames.add(sbd.toString());
					}
				}
			}
		}

		return outColNames.toArray(new String[0]);
	}

	@Override
	public TypeInformation <?>[] getOutColTypes(TableSchema schema) {
		int numCondition = this.conditions == null ? 1 : this.conditions.length;
		List <TypeInformation <?>> outColTypes = new ArrayList <>();

		for (BaseCategoricalStatistics type : this.types) {
			String[] outFeatureNames = this.featureCols;
			if (CategoricalDaysStatistics.CATES_CNT == type) {
				outFeatureNames = this.featureItems;
			}
			for (int ic = 0; ic < numCondition; ic++) {
				for (String featureCol : outFeatureNames) {
					for (String nDay : nDays) {
						TypeInformation <?> outType = null;
						if (type instanceof CategoricalDaysStatistics) {
							outType = ((CategoricalDaysStatistics) type).getOutType();
						}
						if (null == outType) {
							outType = schema.getFieldType(featureCol).get();
						}
						outColTypes.add(outType);
					}
				}
			}
		}
		return outColTypes.toArray(new TypeInformation <?>[0]);
	}
}
