package com.alibaba.alink.common.sql.builtin.agg;


public class RankUdaf extends BaseRankUdaf {

	public RankUdaf() {
		super();
	}

	@Override
	public void accumulate(RankData rankData, Object... values) {
		accumulateTemp(rankData, values);
		rankData.updateRank();
	}
}
