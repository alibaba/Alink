package com.alibaba.alink.common.sql.builtin;

import com.alibaba.alink.common.sql.builtin.agg.*;
import com.alibaba.alink.common.sql.builtin.time.*;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class BuildInAggRegister {
    public static final String EXTEND = "_preceding";
    public static final String CONSIDER_NULL_EXTEND = "_including_null";

    public enum UdafName {
        COUNT("count"),
        SUM("sum"),
        AVG("avg"),
        MIN("min"),
        MAX("max"),
        STD_SAMP("stddev_samp"),
        STD_POP("stddev_pop"),
        VAR_SAMP("var_samp"),
        VAR_POP("var_pop"),
        SKEWNESS("skewness"),
        RANK("rank"),
        DENSE_RANK("dense_rank"),
        ROW_NUMBER("row_number"),
        LAG("lag"),
        LAST_DISTINCT("last_distinct"),
        LAST_TIME("last_time"),
        LAST_VALUE("last_value"),
        LISTAGG("listagg"),
        MODE("mode"),
        SUM_LAST("sum_last"),
        SQUARE_SUM("square_sum"),
        MEDIAN("median"),
        FREQ("freq"),
        IS_EXIST("is_exist");

        public String name;
        UdafName(String name) {
            this.name = name;
        }
    }

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
        env.registerFunction(UdafName.MODE.name, new ModeUdaf(false));
        env.registerFunction(UdafName.MODE.name + EXTEND, new ModeUdaf(true));
        env.registerFunction(UdafName.FREQ.name, new FreqUdaf(false));
        env.registerFunction(UdafName.FREQ.name + EXTEND, new FreqUdaf(true));
        env.registerFunction(UdafName.IS_EXIST.name, new IsExistUdaf());
    }
}