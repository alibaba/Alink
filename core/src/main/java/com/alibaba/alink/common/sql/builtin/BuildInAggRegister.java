package com.alibaba.alink.common.sql.builtin;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.alibaba.alink.common.sql.builtin.agg.AvgUdaf;
import com.alibaba.alink.common.sql.builtin.agg.CountUdaf;
import com.alibaba.alink.common.sql.builtin.agg.DenseRankUdaf;
import com.alibaba.alink.common.sql.builtin.agg.FreqUdaf;
import com.alibaba.alink.common.sql.builtin.agg.IsExistUdaf;
import com.alibaba.alink.common.sql.builtin.agg.LagUdaf;
import com.alibaba.alink.common.sql.builtin.agg.LastDistinctValueUdaf;
import com.alibaba.alink.common.sql.builtin.agg.LastTimeUdaf;
import com.alibaba.alink.common.sql.builtin.agg.LastValueUdaf;
import com.alibaba.alink.common.sql.builtin.agg.ListAggUdaf;
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
import com.alibaba.alink.common.sql.builtin.time.DataFormat;
import com.alibaba.alink.common.sql.builtin.time.FromUnixTime;
import com.alibaba.alink.common.sql.builtin.time.Now;
import com.alibaba.alink.common.sql.builtin.time.ToTimeStamp;
import com.alibaba.alink.common.sql.builtin.time.UnixTimeStamp;

public class BuildInAggRegister {
	public static final String EXTEND = "_preceding";
	public static final String CONSIDER_NULL_EXTEND = "_including_null";

	public static void registerUdf(TableEnvironment env) {
		env.registerFunction("now", new Now());
		env.registerFunction("to_timestamp", new ToTimeStamp());
		env.registerFunction("unix_timestamp", new UnixTimeStamp());
		env.registerFunction("from_unixtime", new FromUnixTime());
		env.registerFunction("date_format_ltz", new DataFormat());
	}

	public static void registerUdaf(StreamTableEnvironment env) {
		env.registerFunction(UdafName.COUNT.name + EXTEND, new CountUdaf(true));
		env.registerFunction(UdafName.SUM.name + EXTEND, new SumUdaf(true));
		env.registerFunction(UdafName.AVG.name + EXTEND, new AvgUdaf(true));
		env.registerFunction(UdafName.MIN.name + EXTEND, new MinUdaf(true));
		env.registerFunction(UdafName.MAX.name + EXTEND, new MaxUdaf(true));
		env.registerFunction(UdafName.STD_SAMP.name + EXTEND, new StddevSampUdaf(true));
		env.registerFunction(UdafName.STD_POP.name + EXTEND, new StddevPopUdaf(true));
		env.registerFunction(UdafName.VAR_SAMP.name + EXTEND, new VarSampUdaf(true));
		env.registerFunction(UdafName.VAR_POP.name + EXTEND, new VarPopUdaf(true));
		env.registerFunction(UdafName.SKEWNESS.name, new SkewnessUdaf());
		env.registerFunction(UdafName.SKEWNESS.name + EXTEND, new SkewnessUdaf(true));
		env.registerFunction(UdafName.SUM_LAST.name, new SumLastUdaf());//sum of last k data
		env.registerFunction(UdafName.SQUARE_SUM.name, new SquareSumUdaf(false));
		env.registerFunction(UdafName.SQUARE_SUM.name + EXTEND, new SquareSumUdaf(true));
		env.registerFunction(UdafName.MEDIAN.name, new MedianUdaf(false));
		env.registerFunction(UdafName.MEDIAN.name + EXTEND, new MedianUdaf(true));
		env.registerFunction(UdafName.RANK.name, new RankUdaf());
		env.registerFunction(UdafName.DENSE_RANK.name, new DenseRankUdaf());
		env.registerFunction(UdafName.ROW_NUMBER.name, new RowNumberUdaf());
		env.registerFunction(UdafName.LAG.name, new LagUdaf());
		env.registerFunction(UdafName.LAG.name + CONSIDER_NULL_EXTEND, new LagUdaf(true));
		env.registerFunction(UdafName.LAST_DISTINCT.name, new LastDistinctValueUdaf());
		env.registerFunction(UdafName.LAST_DISTINCT.name + CONSIDER_NULL_EXTEND, new LastDistinctValueUdaf(true));
		env.registerFunction(UdafName.LAST_TIME.name, new LastTimeUdaf());
		env.registerFunction(UdafName.LAST_VALUE.name, new LastValueUdaf());
		env.registerFunction(UdafName.LAST_VALUE.name + CONSIDER_NULL_EXTEND, new LastValueUdaf(true));
		env.registerFunction(UdafName.LISTAGG.name, new ListAggUdaf());
		env.registerFunction(UdafName.LISTAGG.name + EXTEND, new ListAggUdaf(true));
		env.registerFunction(UdafName.MODE.name, new ModeUdaf(false));
		env.registerFunction(UdafName.MODE.name + EXTEND, new ModeUdaf(true));
		env.registerFunction(UdafName.FREQ.name, new FreqUdaf(false));
		env.registerFunction(UdafName.FREQ.name + EXTEND, new FreqUdaf(true));
		env.registerFunction(UdafName.IS_EXIST.name, new IsExistUdaf());
		env.registerFunction(UdafName.TIMESERIES_AGG.name + EXTEND, new TimeSeriesAgg(true));
		env.registerFunction(UdafName.TIMESERIES_AGG.name, new TimeSeriesAgg(false));
		env.registerFunction(UdafName.CONCAT_AGG.name, new ListAggUdaf());
	}

	public static void registerUdaf(BatchTableEnvironment env) {
		env.registerFunction(UdafName.COUNT.name + EXTEND, new CountUdaf(true));
		env.registerFunction(UdafName.SUM.name + EXTEND, new SumUdaf(true));
		env.registerFunction(UdafName.AVG.name + EXTEND, new AvgUdaf(true));
		env.registerFunction(UdafName.MIN.name + EXTEND, new MinUdaf(true));
		env.registerFunction(UdafName.MAX.name + EXTEND, new MaxUdaf(true));
		env.registerFunction(UdafName.STD_SAMP.name + EXTEND, new StddevSampUdaf(true));
		env.registerFunction(UdafName.STD_POP.name + EXTEND, new StddevPopUdaf(true));
		env.registerFunction(UdafName.VAR_SAMP.name + EXTEND, new VarSampUdaf(true));
		env.registerFunction(UdafName.VAR_POP.name + EXTEND, new VarPopUdaf(true));
		env.registerFunction(UdafName.SKEWNESS.name, new SkewnessUdaf());
		env.registerFunction(UdafName.SUM_LAST.name, new SumLastUdaf());//sum of last k data
		env.registerFunction(UdafName.SQUARE_SUM.name, new SquareSumUdaf(false));
		env.registerFunction(UdafName.SQUARE_SUM.name + EXTEND, new SquareSumUdaf(true));
		env.registerFunction(UdafName.MEDIAN.name, new MedianUdaf(false));
		env.registerFunction(UdafName.MEDIAN.name + EXTEND, new MedianUdaf(true));
		env.registerFunction(UdafName.SKEWNESS.name + EXTEND, new SkewnessUdaf(true));
		env.registerFunction(UdafName.RANK.name, new RankUdaf());
		env.registerFunction(UdafName.DENSE_RANK.name, new DenseRankUdaf());
		env.registerFunction(UdafName.ROW_NUMBER.name, new RowNumberUdaf());
		env.registerFunction(UdafName.LAG.name, new LagUdaf());
		env.registerFunction(UdafName.LAG.name + CONSIDER_NULL_EXTEND, new LagUdaf(true));
		env.registerFunction(UdafName.LAST_DISTINCT.name, new LastDistinctValueUdaf());
		env.registerFunction(UdafName.LAST_DISTINCT.name + CONSIDER_NULL_EXTEND, new LastDistinctValueUdaf(true));
		env.registerFunction(UdafName.LAST_VALUE.name, new LastValueUdaf());
		env.registerFunction(UdafName.LAST_VALUE.name + CONSIDER_NULL_EXTEND, new LastValueUdaf(true));
		env.registerFunction(UdafName.LISTAGG.name, new ListAggUdaf());
		env.registerFunction(UdafName.LISTAGG.name + EXTEND, new ListAggUdaf(true));
		env.registerFunction(UdafName.CONCAT_AGG.name, new ListAggUdaf());
		env.registerFunction(UdafName.CONCAT_AGG.name + EXTEND, new ListAggUdaf(true));
		env.registerFunction(UdafName.MODE.name, new ModeUdaf(false));
		env.registerFunction(UdafName.MODE.name + EXTEND, new ModeUdaf(true));
		env.registerFunction(UdafName.FREQ.name, new FreqUdaf(false));
		env.registerFunction(UdafName.FREQ.name + EXTEND, new FreqUdaf(true));
		env.registerFunction(UdafName.IS_EXIST.name, new IsExistUdaf());
		env.registerFunction(UdafName.TIMESERIES_AGG.name + EXTEND, new TimeSeriesAgg(true));
		env.registerFunction(UdafName.TIMESERIES_AGG.name, new TimeSeriesAgg(false));
	}
}