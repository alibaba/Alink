package com.alibaba.alink.common.fe.define;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.fe.define.over.LatestNCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.over.LatestTimeIntervalCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.over.LatestTimeSlotCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.statistics.BaseCategoricalStatistics;
import com.alibaba.alink.common.fe.define.statistics.CategoricalStatistics;
import com.alibaba.alink.common.fe.define.window.HopWindowCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.window.SessionWindowCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.window.SlotWindowCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.window.TumbleWindowCrossCategoricalStatFeatures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class BaseCrossCategoricalStatFeatures<T extends BaseStatFeatures <T>> extends BaseStatFeatures <T> implements
	Serializable {
	public BaseCategoricalStatistics[] types;
	public String[][] crossFeatureCols;

	public BaseCrossCategoricalStatFeatures() {
		super();
	}

	public BaseCategoricalStatistics[] getCategoricalStatistics() {
		return types;
	}

	public T setCategoricalStatistics(BaseCategoricalStatistics... types) {
		this.types = types;
		return (T) this;
	}

	public String[][] getCrossFeatureCols() {
		return crossFeatureCols;
	}

	public T setCrossFeatureCols(String[]... crossFeatureCols) {
		this.crossFeatureCols = crossFeatureCols;
		return (T) this;
	}

	@Override
	public TypeInformation <?>[] getOutColTypes(TableSchema schema) {
		TypeInformation <?>[] outColTypes = new TypeInformation[this.crossFeatureCols.length * this.types.length];
		int idx = 0;
		for (String[] featureCols : this.crossFeatureCols) {
			for (BaseCategoricalStatistics type : this.types) {
				TypeInformation <?> outType = null;
				if (type instanceof CategoricalStatistics) {
					outType = ((CategoricalStatistics) type).getOutType();
				}
				if (null == outType) {
					outType = Types.STRING;
				}
				outColTypes[idx++] = outType;
			}
		}
		return outColTypes;
	}

	@Override
	public String[] getSimplifiedOutColNames() {
		String[] outColNames = new String[this.crossFeatureCols.length * this.types.length];
		int idx = 0;
		for (String[] featureCols : this.crossFeatureCols) {
			for (BaseCategoricalStatistics type : this.types) {
				outColNames[idx++] = String.format("%s_%s",
					type.name().toLowerCase(),
					String.join("_", featureCols)
				);
			}
		}
		return outColNames;
	}

	@Override
	public BaseStatFeatures <?>[] flattenFeatures() {
		List <BaseStatFeatures <?>> flattenedFeatures = new ArrayList <>();
		if (this instanceof LatestNCrossCategoricalStatFeatures) {
			int[] ns = ((InterfaceNStatFeatures) this).getNumbers();
			if (null == ns && 0 == ns.length) {
				throw new AkIllegalOperatorParameterException("number must be set.");
			}
			for (int n : ns) {
				flattenedFeatures.add(
					new LatestNCrossCategoricalStatFeatures()
						.setNumbers(n)
						.setCrossFeatureCols(this.crossFeatureCols)
						.setCategoricalStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}
		} else if (this instanceof LatestTimeIntervalCrossCategoricalStatFeatures) {
			String[] timeIntervals = ((LatestTimeIntervalCrossCategoricalStatFeatures) this).getTimeIntervals();
			if (null == timeIntervals && 0 == timeIntervals.length) {
				throw new AkIllegalOperatorParameterException("time interval must be set.");
			}
			for (String timeInterval : timeIntervals) {
				flattenedFeatures.add(
					new LatestTimeIntervalCrossCategoricalStatFeatures()
						.setTimeIntervals(timeInterval)
						.setCrossFeatureCols(this.crossFeatureCols)
						.setCategoricalStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}

		} else if (this instanceof LatestTimeSlotCrossCategoricalStatFeatures) {
			String[] timeSlots = ((LatestTimeSlotCrossCategoricalStatFeatures) this).getTimeSlots();
			if (null == timeSlots && 0 == timeSlots.length) {
				throw new AkIllegalOperatorParameterException("time slot must be set.");
			}
			for (String timeSlot : timeSlots) {
				flattenedFeatures.add(
					new LatestTimeSlotCrossCategoricalStatFeatures()
						.setTimeSlots(timeSlot)
						.setCrossFeatureCols(this.crossFeatureCols)
						.setCategoricalStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}
		} else if (this instanceof TumbleWindowCrossCategoricalStatFeatures) {
			String[] windowTimes = ((TumbleWindowCrossCategoricalStatFeatures) this).getWindowTimes();
			if (null == windowTimes && 0 == windowTimes.length) {
				throw new AkIllegalOperatorParameterException("window time must be set.");
			}
			for (String windowTime : windowTimes) {
				flattenedFeatures.add(
					new TumbleWindowCrossCategoricalStatFeatures()
						.setWindowTimes(windowTime)
						.setCrossFeatureCols(this.crossFeatureCols)
						.setCategoricalStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}
		} else if (this instanceof SessionWindowCrossCategoricalStatFeatures) {
			String[] sessionTimes = ((SessionWindowCrossCategoricalStatFeatures) this).getSessionGapTimes();
			if (null == sessionTimes && 0 == sessionTimes.length) {
				throw new AkIllegalOperatorParameterException("session time must be set.");
			}
			for (String sessionTime : sessionTimes) {
				flattenedFeatures.add(
					new SessionWindowCrossCategoricalStatFeatures()
						.setSessionGapTimes(sessionTime)
						.setCrossFeatureCols(this.crossFeatureCols)
						.setCategoricalStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}
		} else if (this instanceof HopWindowCrossCategoricalStatFeatures) {
			String[] windowTimes = ((HopWindowCrossCategoricalStatFeatures) this).getWindowTimes();
			String[] hopTimes = ((HopWindowCrossCategoricalStatFeatures) this).getHopTimes();
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
					new HopWindowCrossCategoricalStatFeatures()
						.setHopTimes(hopTimes[i])
						.setWindowTimes(windowTimes[i])
						.setCrossFeatureCols(this.crossFeatureCols)
						.setCategoricalStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}
		} else if (this instanceof SlotWindowCrossCategoricalStatFeatures) {
			String[] windowTimes = ((SlotWindowCrossCategoricalStatFeatures) this).getWindowTimes();
			String[] stepTimes = ((SlotWindowCrossCategoricalStatFeatures) this).getStepTimes();
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
					new SlotWindowCrossCategoricalStatFeatures()
						.setStepTimes(stepTimes[i])
						.setWindowTimes(windowTimes[i])
						.setCrossFeatureCols(this.crossFeatureCols)
						.setCategoricalStatistics(this.types)
						.setGroupCols(this.groupCols)
				);
			}

		}
		return flattenedFeatures.toArray(new BaseStatFeatures <?>[0]);
	}

}
