package com.alibaba.alink.operator.common.distance;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.similarity.BaseSample;
import org.apache.flink.types.Row;

/**
 * FastCompareCategoricalDistance is used to accelerate the speed of calculating the distance by pre-calculating
 * some extra info of the string.
 */
public interface FastCompareCategoricalDistance<T> {
    BaseSample strInfo(String str, Row row);

    BaseSample textInfo(String strArray, Row row);

    default BaseSample strInfo(String str){
        return strInfo(str, null);
    }

    default BaseSample strInfo(Row row, int strIdx, int... keepIdxs){
        String str = (String) row.getField(strIdx);
        row = TableUtil.getRow(row, keepIdxs);
        return strInfo(str, row);
    }

    default BaseSample textInfo(String str){
        return textInfo(str, null);
    }

    default BaseSample textInfo(Row row, int strIdx, int... keepIdxs){
        String str = (String) row.getField(strIdx);
        row = TableUtil.getRow(row, keepIdxs);
        return textInfo(str, row);
    }

    double calcString(BaseSample<T> left, BaseSample<T> right);

    double calcText(BaseSample<T> left, BaseSample<T> right);

}
