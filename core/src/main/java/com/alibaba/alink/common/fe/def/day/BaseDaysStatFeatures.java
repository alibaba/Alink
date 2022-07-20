package com.alibaba.alink.common.fe.def.day;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.fe.StatisticsAdapter;
import com.alibaba.alink.common.fe.def.statistics.BaseCategoricalStatistics;
import com.alibaba.alink.common.fe.def.statistics.BaseNumericStatistics;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Serializable;

public abstract class BaseDaysStatFeatures<T extends BaseDaysStatFeatures <T>> implements Serializable {

	private final static Gson pGson =
		new GsonBuilder()
			.serializeNulls()
			.disableHtmlEscaping()
			.serializeSpecialFloatingPointValues()
			.registerTypeAdapter(CategoricalDaysStatistics.class,
				new StatisticsAdapter <CategoricalDaysStatistics>())
			.registerTypeAdapter(NumericDaysStatistics.class,
				new StatisticsAdapter <NumericDaysStatistics>())
			.registerTypeAdapter(BaseDaysStatFeatures.class,
				new BaseDaysFeaturesAdapter())
			.registerTypeAdapter(BaseNumericStatistics.class,
				new StatisticsAdapter <BaseNumericStatistics>())
			.registerTypeAdapter(BaseCategoricalStatistics.class,
				new StatisticsAdapter <BaseCategoricalStatistics>())
			.create();

	public String[] groupCols;
	public String[] nDays;
	public String conditionCol; //option
	public String[][] conditions; // option

	//for params to json
	public BaseDaysStatFeatures() {
		this.groupCols = null;
	}

	public T setGroupCols(String... groupCols) {
		this.groupCols = groupCols;
		return (T) this;
	}

	public String[] getGroupCols() {
		return groupCols;
	}

	public T setNDays(String... nDays) {
		this.nDays = nDays;
		return (T) this;
	}

	public String[] getNDays() {
		if (nDays == null) {
			throw new AkIllegalOperatorParameterException("nDays must be set.");
		}
		return nDays;
	}

	public String getConditionCol() {
		return conditionCol;
	}

	public T setConditionCol(String conditionCol) {
		this.conditionCol = conditionCol;
		return (T) this;
	}

	public String[][] getConditions() {
		return conditions;
	}

	public T setConditions(String[]... conditions) {
		this.conditions = conditions;
		return (T) this;
	}

	public static String toJson(Object obj) {
		return pGson.toJson(obj);
	}

	public static <T> T fromJson(String s, Class <T> type) {
		return pGson.fromJson(s, type);
	}

	public abstract String[] getOutColNames();

	public abstract TypeInformation <?>[] getOutColTypes(TableSchema schema);

}
