package com.alibaba.alink.common.fe.def.day;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.fe.def.statistics.BaseCategoricalStatistics;
import com.alibaba.alink.common.fe.def.statistics.BaseNumericStatistics;
import com.alibaba.alink.operator.batch.feature.GenerateFeatureOfLatestNDaysBatchOp;

import java.util.ArrayList;
import java.util.List;

public class NDaysNumericStatFeatures
	extends BaseDaysStatFeatures <NDaysNumericStatFeatures> {

	public BaseNumericStatistics[] types;
	public String[] featureCols;

	public NDaysNumericStatFeatures() {
		super();
	}

	public BaseNumericStatistics[] getNumericStatistics() {
		return types;
	}

	public NDaysNumericStatFeatures setNumericDaysStatistics(BaseNumericStatistics... numericDaysStatistics) {
		this.types = numericDaysStatistics;
		return this;
	}

	public String[] getFeatureCols() {
		return featureCols;
	}

	public NDaysNumericStatFeatures setFeatureCols(String... featureCols) {
		this.featureCols = featureCols;
		return this;
	}

	//type[__col(_items)]_days[__conditionCol_condition][__groups]
	public String[] getOutColNames() {
		int numCondition = this.conditions == null ? 1 : this.conditions.length;
		List <String> outColNames = new ArrayList <>();

		String deli = "__";
		for (BaseNumericStatistics type : this.types) {
			String[] outFeatureNames = this.featureCols;
			for (int ic = 0; ic < numCondition; ic++) {
				for (String featureItem : outFeatureNames) {
					for (String nDay : nDays) {
						StringBuilder sbd = new StringBuilder();
						sbd.append(type.name().toLowerCase());
						if (NumericDaysStatistics.TOTAL_COUNT != type) {
							sbd.append(deli)
								.append(featureItem);
						}
						sbd.append(deli)
							.append(nDay);
						if (this.conditions != null) {
							if (this.conditions[ic] != null && this.conditions[ic].length != 0) {
								sbd.append(deli)
									.append(this.conditionCol)
									.append(deli)
									.append(String.join("_", this.conditions[ic]));
							}
						}

						if (this.groupCols != null) {
							if (this.groupCols.length != 1 || !this.groupCols[0].equals(
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
		TypeInformation <?>[] outColTypes = new TypeInformation[numCondition * this.nDays.length
			* this.featureCols.length
			* this.types.length];
		int idx = 0;

		for (BaseNumericStatistics type : this.types) {
			for (int ic = 0; ic < numCondition; ic++) {
				for (String featureCol : this.featureCols) {
					for (String nDay : nDays) {
						TypeInformation <?> outType = null;
						if (type instanceof NumericDaysStatistics) {
							outType = ((NumericDaysStatistics) type).getOutType();
						}
						if (null == outType) {
							outType = schema.getFieldType(featureCol).get();
						}
						outColTypes[idx++] = outType;
					}
				}
			}
		}

		return outColTypes;
	}

}
