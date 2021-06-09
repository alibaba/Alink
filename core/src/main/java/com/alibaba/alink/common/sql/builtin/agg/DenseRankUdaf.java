package com.alibaba.alink.common.sql.builtin.agg;


public class DenseRankUdaf extends BaseRankUdaf {

	public DenseRankUdaf() {
		super();
	}

	@Override
	public void accumulate(RankData rankData, Object... values) {
		accumulateTemp(rankData, values);
		rankData.updateDenseRank();
	}
}
