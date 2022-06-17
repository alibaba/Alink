package com.alibaba.alink.common.fe.def;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.fe.def.over.LatestNNumericStatFeatures;
import com.alibaba.alink.common.fe.def.over.LatestTimeIntervalNumericStatFeatures;
import com.alibaba.alink.common.fe.def.over.LatestTimeSlotNumericStatFeatures;
import com.alibaba.alink.common.fe.def.statistics.BaseNumericStatistics;
import com.alibaba.alink.common.fe.def.statistics.NumericStatistics;
import com.alibaba.alink.common.fe.def.window.HopWindowNumericStatFeatures;
import com.alibaba.alink.common.fe.def.window.SessionWindowNumericStatFeatures;
import com.alibaba.alink.common.fe.def.window.SlotWindowNumericStatFeatures;
import com.alibaba.alink.common.fe.def.window.TumbleWindowNumericStatFeatures;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseNumericStatFeatures<T extends BaseStatFeatures <T>> extends BaseStatFeatures <T> {
	public BaseNumericStatistics[] types;
	public String[] featureCols;

	public BaseNumericStatFeatures() {
		super();
	}

	public BaseNumericStatistics[] getNumericStatistics() {
		return types;
	}

	public T setNumericStatistics(BaseNumericStatistics... types) {
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
			for (BaseNumericStatistics type : this.types) {
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
			for (BaseNumericStatistics type : this.types) {
				TypeInformation <?> outType = null;
				if (type instanceof NumericStatistics) {
					outType = ((NumericStatistics) type).getOutType();
				}
				if(type instanceof NumericStatistics.ConcatAgg){
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
		if (this instanceof LatestNNumericStatFeatures) {
			int[] ns = ((InterfaceNStatFeatures) this).getNumbers();
			if (null == ns && 0 == ns.length) {
				throw new RuntimeException("number must be set.");
			}
			for (int n : ns) {
				flattenedFeatures.add(
					new LatestNNumericStatFeatures()
						.setNumbers(n)
						.setFeatureCols(this.featureCols)
						.setNumericStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}
		} else if (this instanceof LatestTimeIntervalNumericStatFeatures) {
			String[] timeIntervals = ((LatestTimeIntervalNumericStatFeatures) this).getTimeIntervals();
			if (null == timeIntervals && 0 == timeIntervals.length) {
				throw new RuntimeException("time interval must be set.");
			}
			for (String timeInterval : timeIntervals) {
				flattenedFeatures.add(
					new LatestTimeIntervalNumericStatFeatures()
						.setTimeIntervals(timeInterval)
						.setFeatureCols(this.featureCols)
						.setNumericStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}

		} else if (this instanceof LatestTimeSlotNumericStatFeatures) {
			String[] timeSlots = ((LatestTimeSlotNumericStatFeatures) this).getTimeSlots();
			if (null == timeSlots && 0 == timeSlots.length) {
				throw new RuntimeException("time slot must be set.");
			}
			for (String timeSlot : timeSlots) {
				flattenedFeatures.add(
					new LatestTimeSlotNumericStatFeatures()
						.setTimeSlots(timeSlot)
						.setFeatureCols(this.featureCols)
						.setNumericStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}
		} else if (this instanceof TumbleWindowNumericStatFeatures) {
			String[] windowTimes = ((TumbleWindowNumericStatFeatures) this).getWindowTimes();
			if (null == windowTimes && 0 == windowTimes.length) {
				throw new RuntimeException("window time must be set.");
			}
			for (String windowTime : windowTimes) {
				flattenedFeatures.add(
					new TumbleWindowNumericStatFeatures()
						.setWindowTimes(windowTime)
						.setFeatureCols(this.featureCols)
						.setNumericStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}
		} else if (this instanceof SessionWindowNumericStatFeatures) {
			String[] sessionTimes = ((SessionWindowNumericStatFeatures) this).getSessionGapTimes();
			if (null == sessionTimes && 0 == sessionTimes.length) {
				throw new RuntimeException("session time must be set.");
			}
			for (String sessionTime : sessionTimes) {
				flattenedFeatures.add(
					new SessionWindowNumericStatFeatures()
						.setSessionGapTimes(sessionTime)
						.setFeatureCols(this.featureCols)
						.setNumericStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}
		} else if (this instanceof HopWindowNumericStatFeatures) {
			String[] windowTimes = ((HopWindowNumericStatFeatures) this).getWindowTimes();
			String[] hopTimes = ((HopWindowNumericStatFeatures) this).getHopTimes();
			if (null == windowTimes && 0 == windowTimes.length) {
				throw new RuntimeException("window time must be set.");
			}
			if (null == hopTimes && 0 == hopTimes.length) {
				throw new RuntimeException("hop time must be set.");
			}
			if (windowTimes.length != hopTimes.length) {
				throw new RuntimeException("hopTimes size must be equal with windowTimes.");
			}
			for (int i = 0; i < windowTimes.length; i++) {
				flattenedFeatures.add(
					new HopWindowNumericStatFeatures()
						.setHopTimes(hopTimes[i])
						.setWindowTimes(windowTimes[i])
						.setFeatureCols(this.featureCols)
						.setNumericStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}
		} else if (this instanceof SlotWindowNumericStatFeatures) {
			String[] windowTimes = ((SlotWindowNumericStatFeatures) this).getWindowTimes();
			String[] stepTimes = ((SlotWindowNumericStatFeatures) this).getStepTimes();
			if (null == windowTimes && 0 == windowTimes.length) {
				throw new RuntimeException("window time must be set.");
			}
			if (null == stepTimes && 0 == stepTimes.length) {
				throw new RuntimeException("step time must be set.");
			}
			if (windowTimes.length != stepTimes.length) {
				throw new RuntimeException("stepTimes size must be equal with windowTimes.");
			}
			for (int i = 0; i < windowTimes.length; i++) {
				flattenedFeatures.add(
					new SlotWindowNumericStatFeatures()
						.setStepTimes(stepTimes[i])
						.setWindowTimes(windowTimes[i])
						.setFeatureCols(this.featureCols)
						.setNumericStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}
		}
		return flattenedFeatures.toArray(new BaseStatFeatures <?>[0]);
	}

}
