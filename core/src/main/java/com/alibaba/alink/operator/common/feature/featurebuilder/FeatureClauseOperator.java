package com.alibaba.alink.operator.common.feature.featurebuilder;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.sql.builtin.agg.AvgUdaf;
import com.alibaba.alink.common.sql.builtin.agg.BaseUdaf;
import com.alibaba.alink.common.sql.builtin.agg.CountUdaf;
import com.alibaba.alink.common.sql.builtin.agg.DenseRankUdaf;
import com.alibaba.alink.common.sql.builtin.agg.FreqUdaf;
import com.alibaba.alink.common.sql.builtin.agg.IsExistUdaf;
import com.alibaba.alink.common.sql.builtin.agg.LagUdaf;
import com.alibaba.alink.common.sql.builtin.agg.LastDistinctValueUdaf;
import com.alibaba.alink.common.sql.builtin.agg.LastTimeUdaf;
import com.alibaba.alink.common.sql.builtin.agg.LastValueUdaf;
import com.alibaba.alink.common.sql.builtin.agg.ListAggUdaf;
import com.alibaba.alink.common.sql.builtin.agg.MTableAgg;
import com.alibaba.alink.common.sql.builtin.agg.MaxUdaf;
import com.alibaba.alink.common.sql.builtin.agg.MedianUdaf;
import com.alibaba.alink.common.sql.builtin.agg.MinUdaf;
import com.alibaba.alink.common.sql.builtin.agg.ModeUdaf;
import com.alibaba.alink.common.sql.builtin.agg.RankUdaf;
import com.alibaba.alink.common.sql.builtin.agg.RowNumberUdaf;
import com.alibaba.alink.common.sql.builtin.agg.SkewnessUdaf;
import com.alibaba.alink.common.sql.builtin.agg.SquareSumUdaf;
import com.alibaba.alink.common.sql.builtin.agg.StddevPopUdaf;
import com.alibaba.alink.common.sql.builtin.agg.StddevSampUdaf;
import com.alibaba.alink.common.sql.builtin.agg.SumLastUdaf;
import com.alibaba.alink.common.sql.builtin.agg.SumUdaf;
import com.alibaba.alink.common.sql.builtin.agg.TimeSeriesAgg;
import com.alibaba.alink.common.sql.builtin.agg.VarPopUdaf;
import com.alibaba.alink.common.sql.builtin.agg.VarSampUdaf;
import com.alibaba.alink.common.utils.JsonConverter;

import static com.alibaba.alink.operator.common.feature.featurebuilder.WindowResColType.RES_TYPE;

/**
 * The udaf is used in batch, the res type is used both batch and stream.
 */
public enum FeatureClauseOperator {
	/**
	 * sum of selected column in window
	 */
	SUM(RES_TYPE, new SumUdaf()),

	SUM_PRECEDING(RES_TYPE, new SumUdaf(true)),
	/**
	 * calc data number in window
	 */
	COUNT(Types.LONG, new CountUdaf()),

	COUNT_PRECEDING(Types.LONG, new CountUdaf(true)),
	/**
	 * mean of selected column in window
	 */
	AVG(RES_TYPE, new AvgUdaf()),

	AVG_PRECEDING(RES_TYPE, new AvgUdaf(true)),

	MIN(RES_TYPE, new MinUdaf()),

	MIN_PRECEDING(RES_TYPE, new MinUdaf(true)),

	MAX(RES_TYPE, new MaxUdaf()),

	MAX_PRECEDING(RES_TYPE, new MaxUdaf(true)),

	STDDEV_SAMP(RES_TYPE, new StddevSampUdaf()),

	STDDEV_SAMP_PRECEDING(RES_TYPE, new StddevSampUdaf(true)),

	STDDEV_POP(RES_TYPE, new StddevPopUdaf()),

	STDDEV_POP_PRECEDING(RES_TYPE, new StddevPopUdaf(true)),

	VAR_SAMP(RES_TYPE, new VarSampUdaf()),

	VAR_SAMP_PRECEDING(RES_TYPE, new VarSampUdaf(true)),

	VAR_POP(RES_TYPE, new VarPopUdaf()),

	VAR_POP_PRECEDING(RES_TYPE, new VarPopUdaf(true)),

	SKEWNESS(RES_TYPE, new SkewnessUdaf()),

	SKEWNESS_PRECEDING(RES_TYPE, new SkewnessUdaf(true)),

	LAG(RES_TYPE, new LagUdaf()),

	LAG_INCLUDING_NULL(RES_TYPE, new LagUdaf(true)),

	/**
	 * distinct of selected column in window
	 */
	LAST_DISTINCT(RES_TYPE, new LastDistinctValueUdaf()),

	LAST_DISTINCT_INCLUDING_NULL(RES_TYPE, new LastDistinctValueUdaf(true)),

	LAST_VALUE(RES_TYPE, new LastValueUdaf()),

	LAST_VALUE_INCLUDING_NULL(RES_TYPE, new LastValueUdaf(true)),

	/**
	 * latest data and time
	 */
	LAST_TIME(Types.SQL_TIMESTAMP, new LastTimeUdaf()),

	LISTAGG(Types.STRING, new ListAggUdaf()),

	LISTAGG_PRECEDING(Types.STRING, new ListAggUdaf(true)),

	CONCAT_AGG(Types.STRING, new ListAggUdaf()),

	CONCAT_AGG_PRECEDING(Types.STRING, new ListAggUdaf(true)),

	MODE(RES_TYPE, new ModeUdaf()),

	MODE_PRECEDING(RES_TYPE, new ModeUdaf(true)),

	SUM_LAST(RES_TYPE, new SumLastUdaf()),

	SQUARE_SUM(RES_TYPE, new SquareSumUdaf()),

	SQUARE_SUM_PRECEDING(RES_TYPE, new SquareSumUdaf(true)),

	MEDIAN(RES_TYPE, new MedianUdaf()),

	MEDIAN_PRECEDING(RES_TYPE, new MedianUdaf(true)),

	FREQ(Types.LONG, new FreqUdaf()),

	FREQ_PRECEDING(Types.LONG, new FreqUdaf(true)),

	IS_EXIST(Types.BOOLEAN, new IsExistUdaf()),

	RANK(Types.LONG, new RankUdaf()),

	DENSE_RANK(Types.LONG, new DenseRankUdaf()),

	ROW_NUMBER(Types.LONG, new RowNumberUdaf()),

	CONCAT(Types.STRING, null),

	TIMESERIES_AGG(Types.STRING, new TimeSeriesAgg(false)),

	TIMESERIES_AGG_PRECEDING(Types.STRING, new TimeSeriesAgg(true)),

	MTABLE_AGG_PRECEDING(AlinkTypes.M_TABLE, new MTableAgg(true)),

	MTABLE_AGG(AlinkTypes.M_TABLE, new MTableAgg(false));

	private final TypeInformation<?> resType;
	private final BaseUdaf calc;

	FeatureClauseOperator(TypeInformation<?> resType, BaseUdaf calc) {
		this.resType = resType;
		this.calc = calc;
	}

	public TypeInformation<?> getResType() {
		return resType;
	}

	public BaseUdaf getCalc() {
		return JsonConverter.fromJson(JsonConverter.toJson(calc), calc.getClass());
	}

	}
