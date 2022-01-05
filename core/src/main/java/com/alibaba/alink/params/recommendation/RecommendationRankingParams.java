package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.nlp.HasTopNDv10;
import com.alibaba.alink.params.shared.colname.HasOutputColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

/**
 * common parameters of RecommendationRank.
 */
public interface RecommendationRankingParams<T>
    extends HasOutputColDefaultAsNull <T>,
    HasReservedColsDefaultAsNull<T>,
    HasTopNDv10 <T> {

    ParamInfo<String> M_TABLE_COL = ParamInfoFactory
        .createParamInfo("mTableCol", String.class)
        .setDescription("recall list col name")
        .setRequired()
        .build();

    default String getMTableCol(){
        return get(M_TABLE_COL);
    }

    default T setMTableCol(String value){
        return set(M_TABLE_COL, value);
    }

    /**
     * @cn-name 用来排序的得分列
     * @cn 用来排序的得分列
     */
    ParamInfo <String> RANKING_COL = ParamInfoFactory
        .createParamInfo("rankingCol", String.class)
        .setDescription("MTable score column name.")
		.setAlias(new String[]{"rankingScoreCol"})
        .setHasDefaultValue(null)
        .build();

    default String getRankingCol() {
        return get(RANKING_COL);
    }

    default T setRankingCol(String value) {
        return set(RANKING_COL, value);
    }
}
