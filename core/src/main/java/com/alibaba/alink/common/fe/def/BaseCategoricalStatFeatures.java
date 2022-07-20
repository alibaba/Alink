package com.alibaba.alink.common.fe.def;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.fe.def.over.LatestNCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.over.LatestTimeIntervalCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.over.LatestTimeSlotCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.statistics.BaseCategoricalStatistics;
import com.alibaba.alink.common.fe.def.statistics.CategoricalStatistics;
import com.alibaba.alink.common.fe.def.window.HopWindowCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.window.SessionWindowCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.window.SlotWindowCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.window.SlotWindowNumericStatFeatures;
import com.alibaba.alink.common.fe.def.window.TumbleWindowCategoricalStatFeatures;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseCategoricalStatFeatures<T extends BaseStatFeatures <T>> extends BaseStatFeatures <T> {
	public BaseCategoricalStatistics[] types;
	public String[] featureCols;

	public BaseCategoricalStatFeatures() {
		super();
	}

	public BaseCategoricalStatistics[] getCategoricalStatistics() {
		return types;
	}

	public T setCategoricalStatistics(BaseCategoricalStatistics... types) {
		this.types = types;
		return (T) this;
	}

	public String[] getFeatureCols() {
		return featureCols;
	}

	public T setFeatureCols(String... featureCols) {
		this.featureCols = featureCols;
		return (T) this;
	}

	@Override
	public String[] getSimplifiedOutColNames() {
		String[] outColNames = new String[this.featureCols.length * this.types.length];
		int idx = 0;
		for (String featureCol : this.featureCols) {
			for (BaseCategoricalStatistics type : this.types) {
				outColNames[idx++] = String.format("%s_%s",
					type.name().toLowerCase(),
					featureCol
				);
			}
		}
		return outColNames;
	}

	@Override
	public TypeInformation <?>[] getOutColTypes(TableSchema schema) {
		TypeInformation <?>[] outColTypes = new TypeInformation[this.featureCols.length * this.types.length];
		int idx = 0;
		for (String featureCol : this.featureCols) {
			for (BaseCategoricalStatistics type : this.types) {
				TypeInformation <?> outType = null;
				if (type instanceof CategoricalStatistics) {
					outType = ((CategoricalStatistics) type).getOutType();
				}
				if(type instanceof CategoricalStatistics.KvStat || type instanceof  CategoricalStatistics.ConcatAgg){
					outType = Types.STRING;
				}
				if (null == outType) {
					outType = schema.getFieldType(featureCol).get();
				}
				outColTypes[idx++] = outType;
			}
		}
		return outColTypes;
	}

	@Override
	public BaseStatFeatures <?>[] flattenFeatures() {
		List <BaseStatFeatures <?>> flattenedFeatures = new ArrayList <>();
		if (this instanceof LatestNCategoricalStatFeatures) {
			int[] ns = ((InterfaceNStatFeatures) this).getNumbers();
			if (null == ns && 0 == ns.length) {
				throw new AkIllegalOperatorParameterException("number must be set.");
			}
			for (int n : ns) {
				flattenedFeatures.add(
					new LatestNCategoricalStatFeatures()
						.setNumbers(n)
						.setFeatureCols(this.featureCols)
						.setCategoricalStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}
		} else if (this instanceof LatestTimeIntervalCategoricalStatFeatures) {
			String[] timeIntervals = ((LatestTimeIntervalCategoricalStatFeatures) this).getTimeIntervals();
			if (null == timeIntervals && 0 == timeIntervals.length) {
				throw new AkIllegalOperatorParameterException("time interval must be set.");
			}
			for (String timeInterval : timeIntervals) {
				flattenedFeatures.add(
					new LatestTimeIntervalCategoricalStatFeatures()
						.setTimeIntervals(timeInterval)
						.setFeatureCols(this.featureCols)
						.setCategoricalStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}

		} else if (this instanceof LatestTimeSlotCategoricalStatFeatures) {
			String[] timeSlots = ((LatestTimeSlotCategoricalStatFeatures) this).getTimeSlots();
			if (null == timeSlots && 0 == timeSlots.length) {
				throw new AkIllegalOperatorParameterException("time slot must be set.");
			}
			for (String timeSlot : timeSlots) {
				flattenedFeatures.add(
					new LatestTimeSlotCategoricalStatFeatures()
						.setTimeSlots(timeSlot)
						.setFeatureCols(this.featureCols)
						.setCategoricalStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}
		} else if (this instanceof TumbleWindowCategoricalStatFeatures) {
			String[] windowTimes = ((TumbleWindowCategoricalStatFeatures) this).getWindowTimes();
			if (null == windowTimes && 0 == windowTimes.length) {
				throw new AkIllegalOperatorParameterException("window time must be set.");
			}
			for (String windowTime : windowTimes) {
				flattenedFeatures.add(
					new TumbleWindowCategoricalStatFeatures()
						.setWindowTimes(windowTime)
						.setFeatureCols(this.featureCols)
						.setCategoricalStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}
		} else if (this instanceof SessionWindowCategoricalStatFeatures) {
			String[] sessionTimes = ((SessionWindowCategoricalStatFeatures) this).getSessionGapTimes();
			if (null == sessionTimes && 0 == sessionTimes.length) {
				throw new AkIllegalOperatorParameterException("session time must be set.");
			}
			for (String sessionTime : sessionTimes) {
				flattenedFeatures.add(
					new SessionWindowCategoricalStatFeatures()
						.setSessionGapTimes(sessionTime)
						.setFeatureCols(this.featureCols)
						.setCategoricalStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}
		} else if (this instanceof HopWindowCategoricalStatFeatures) {
			String[] windowTimes = ((HopWindowCategoricalStatFeatures) this).getWindowTimes();
			String[] hopTimes = ((HopWindowCategoricalStatFeatures) this).getHopTimes();
			if (null == windowTimes && 0 == windowTimes.length) {
				throw new AkIllegalOperatorParameterException("window time must be set.");
			}
			if (null == hopTimes && 0 == hopTimes.length) {
				throw new AkIllegalOperatorParameterException("hop time must be set.");
			}
			if (windowTimes.length != hopTimes.length) {
				throw new AkIllegalOperatorParameterException("hopTimes size must be equal with windowTimes.");
			}
			for (int i = 0; i < windowTimes.length; i++) {
				flattenedFeatures.add(
					new HopWindowCategoricalStatFeatures()
						.setHopTimes(hopTimes[i])
						.setWindowTimes(windowTimes[i])
						.setFeatureCols(this.featureCols)
						.setCategoricalStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}
		} else if (this instanceof SlotWindowCategoricalStatFeatures) {
			String[] windowTimes = ((SlotWindowCategoricalStatFeatures) this).getWindowTimes();
			String[] stepTimes = ((SlotWindowCategoricalStatFeatures) this).getStepTimes();
			if (null == windowTimes && 0 == windowTimes.length) {
				throw new AkIllegalOperatorParameterException("window time must be set.");
			}
			if (null == stepTimes && 0 == stepTimes.length) {
				throw new AkIllegalOperatorParameterException("step time must be set.");
			}
			if (windowTimes.length != stepTimes.length) {
				throw new AkIllegalOperatorParameterException("stepTimes size must be equal with windowTimes.");
			}
			for (int i = 0; i < windowTimes.length; i++) {
				flattenedFeatures.add(
					new SlotWindowCategoricalStatFeatures()
						.setStepTimes(stepTimes[i])
						.setWindowTimes(windowTimes[i])
						.setFeatureCols(this.featureCols)
						.setCategoricalStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}

		}
		return flattenedFeatures.toArray(new BaseStatFeatures <?>[0]);
	}

}
