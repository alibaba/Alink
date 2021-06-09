package com.alibaba.alink.common.sql.builtin.agg;


public class RowNumberUdaf extends BaseRankUdaf {

	public RowNumberUdaf() {
		super();
	}

	@Override
	public void accumulate(RankData rankData, Object... values) {
		accumulateTemp(rankData, values);
		rankData.updateRowNumber();;
	}
}
