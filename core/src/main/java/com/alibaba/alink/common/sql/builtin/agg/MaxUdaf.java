package com.alibaba.alink.common.sql.builtin.agg;

public class MaxUdaf extends MinUdaf {

    public MaxUdaf() {
        super();
    }

    public MaxUdaf(boolean dropLast) {
        super(dropLast);
    }

    @Override
    public MinMaxData createAccumulator() {
        return new MinMaxData(false, excludeLast);
    }
}
