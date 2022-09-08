package com.alibaba.alink.common.fe.define;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.fe.BaseFeaturesAdapter;
import com.alibaba.alink.common.fe.StatisticsAdapter;
import com.alibaba.alink.common.fe.define.statistics.BaseCategoricalStatistics;
import com.alibaba.alink.common.fe.define.statistics.BaseNumericStatistics;
import com.alibaba.alink.common.fe.define.statistics.CategoricalStatistics;
import com.alibaba.alink.common.fe.define.statistics.CategoricalStatistics.LastN;
import com.alibaba.alink.common.fe.define.statistics.NumericStatistics;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Serializable;

public abstract class BaseStatFeatures<T extends BaseStatFeatures <T>> implements Serializable {

	public final static String LATEST_N = "ln";
	public final static String LATEST_TIME_INTERVAL = "lti";
	public final static String LATEST_TIME_SLOT = "lts";
	public final static String CATEGORY = "c";
	public final static String NUMBER = "n";
	public final static String CROSS_CATEGORY = "cc";
	public final static String HOP_WINDOW = "hw";
	public final static String SESSION_WINDOW = "sw";
	public final static String TUMBLE_WINDOW = "tw";
	public final static String SLOT_WINDOW = "slw";

	private final static Gson pGson =
		new GsonBuilder()
			.serializeNulls()
			.disableHtmlEscaping()
			.serializeSpecialFloatingPointValues()
			.registerTypeAdapter(CategoricalStatistics.class,
				new StatisticsAdapter <CategoricalStatistics>())
			.registerTypeAdapter(BaseCategoricalStatistics.class,
				new StatisticsAdapter <BaseCategoricalStatistics>())
			.registerTypeAdapter(CategoricalStatistics.FirstN.class,
				new StatisticsAdapter <CategoricalStatistics.FirstN>())
			.registerTypeAdapter(LastN.class,
				new StatisticsAdapter <LastN>())
			.registerTypeAdapter(NumericStatistics.class,
				new StatisticsAdapter <NumericStatistics>())
			.registerTypeAdapter(BaseNumericStatistics.class,
				new StatisticsAdapter <BaseNumericStatistics>())
			.registerTypeAdapter(NumericStatistics.FirstN.class,
				new StatisticsAdapter <NumericStatistics.FirstN>())
			.registerTypeAdapter(NumericStatistics.LastN.class,
				new StatisticsAdapter <NumericStatistics.LastN>())
			.registerTypeAdapter(BaseStatFeatures.class,
				new BaseFeaturesAdapter <BaseFeaturesAdapter>())
			.registerTypeAdapter(InterfaceLatestStatFeatures.class,
				new BaseFeaturesAdapter <InterfaceLatestStatFeatures>())
			.registerTypeAdapter(InterfaceWindowStatFeatures.class,
				new BaseFeaturesAdapter <InterfaceWindowStatFeatures>())
			.create();

	public String[] groupCols;

	//for params to json
	public BaseStatFeatures() {
		this.groupCols = null;
	}

	public String[] getGroupCols() {
		return groupCols;
	}

	public T setGroupCols(String... groupCols) {
		this.groupCols = groupCols;
		return (T) this;
	}

	public String[] getOutColNames() {
		String[] simplifiedOutColNames = getSimplifiedOutColNames();
		String[] outColNames = new String[simplifiedOutColNames.length];
		String prefix = getColPrefix();
		for (int i = 0; i < simplifiedOutColNames.length; i++) {
			outColNames[i] = String.join("_", prefix, simplifiedOutColNames[i]);
		}
		return outColNames;
	}

	public String[] getSimplifiedOutColNames() {
		throw new AkUnsupportedOperationException("It is not support yet!getOutColNames");
	}

	public String getColPrefix() {
		return String.format("%s_%s", getStatisticsName(), String.join("_", this.groupCols));
	}

	public TypeInformation <?>[] getOutColTypes(TableSchema schema) {
		throw new AkUnsupportedOperationException("It is not support yet! getOutColTypes");
	}

	public BaseStatFeatures <?>[] flattenFeatures() {
		throw new AkUnsupportedOperationException("It is not support yet! expandFeatures");
	}

	public static String toJson(Object obj) {
		return pGson.toJson(obj);
	}

	public static <T> T fromJson(String s, Class <T> type) {
		return pGson.fromJson(s, type);
	}

	//for single feature.
	public String getStatisticsName() {
		String windowName = "";
		String dataType = "";
		String params = "";
		if (this instanceof InterfaceHopWindowStatFeatures) {
			windowName = HOP_WINDOW;
			params = String.join("_",
				((InterfaceHopWindowStatFeatures) this).getWindowTimes()[0],
				((InterfaceHopWindowStatFeatures) this).getHopTimes()[0]);
		} else if (this instanceof InterfaceSessionWindowStatFeatures) {
			windowName = SESSION_WINDOW;
			params = ((InterfaceSessionWindowStatFeatures) this).getSessionGapTimes()[0];
		} else if (this instanceof InterfaceTumbleWindowStatFeatures) {
			windowName = TUMBLE_WINDOW;
			params = ((InterfaceTumbleWindowStatFeatures) this).getWindowTimes()[0];
		} else if (this instanceof InterfaceNStatFeatures) {
			windowName = LATEST_N;
			params = String.valueOf(((InterfaceNStatFeatures) this).getNumbers()[0]);
		} else if (this instanceof InterfaceTimeIntervalStatFeatures) {
			windowName = LATEST_TIME_INTERVAL;
			params = ((InterfaceTimeIntervalStatFeatures) this).getTimeIntervals()[0];
		} else if (this instanceof InterfaceTimeSlotStatFeatures) {
			windowName = LATEST_TIME_SLOT;
			params = ((InterfaceTimeSlotStatFeatures) this).getTimeSlots()[0];
		} else if (this instanceof InterfaceSlotWindowStatFeatures) {
			windowName = SLOT_WINDOW;
			params = String.join("_",
				((InterfaceSlotWindowStatFeatures) this).getWindowTimes()[0],
				((InterfaceSlotWindowStatFeatures) this).getStepTimes()[0]);
		}

		if (this instanceof BaseNumericStatFeatures) {
			dataType = NUMBER;
		} else if (this instanceof BaseCategoricalStatFeatures) {
			dataType = CATEGORY;
		} else if (this instanceof BaseCrossCategoricalStatFeatures) {
			dataType = CROSS_CATEGORY;
		}

		return String.join("_", dataType, windowName, params);
	}
}
