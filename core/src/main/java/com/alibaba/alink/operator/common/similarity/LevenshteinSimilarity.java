package com.alibaba.alink.operator.common.similarity;

import com.alibaba.alink.operator.common.distance.LevenshteinDistance;
import org.apache.flink.types.Row;

/**
 * Calculate the Levenshtein Distance.
 * Levenshtein metric: the minimum number of single-character edits (insertions, deletions or substitutions)
 * required to change one word into the other.
 */
public class LevenshteinSimilarity extends Similarity<Double>{

    public LevenshteinSimilarity() {
        this.distance = new LevenshteinDistance();
    }

    /**
     * similarity = 1.0 - Normalized Distance
     * Override the similarity function of Similarity.
     * @param left one of the string to calculate.
     * @param right one of the string to calculate.
     * @return the similarity between left and right.
     */
    @Override
    public double similarity(String left, String right) {
        int len = Math.max(left.length(), right.length());
        if (len == 0) {
            return 0.0;
        }
        return 1.0 - distance.calc(left, right) / (double) len;
    }

    @Override
    public double similarity(String[] left, String[] right) {
        int len = Math.max(left.length, right.length);
        if (len == 0) {
            return 0.0;
        }
        return 1.0 - distance.calc(left, right) / (double) len;
    }

    @Override
    public BaseSample strInfo(String str, Row row) {
        return new BaseSample(str, row, null);
    }

    @Override
    public BaseSample textInfo(String strArray, Row row) {
        return new BaseSample(strArray, row, null);
    }

    @Override
    public double calcString(BaseSample<Double> left, BaseSample<Double> right) {
        int len = Math.max(left.getStr().length(), right.getStr().length());
        if (len == 0) {
            return 0.0;
        }
        return 1.0 - ((LevenshteinDistance) distance).calcString(left, right) / (double) len;
    }

    @Override
    public double calcText(BaseSample<Double> left, BaseSample<Double> right) {
        int len = Math.max(BaseSample.split(left.getStr()).length, BaseSample.split(right.getStr()).length);
        if (len == 0) {
            return 0.0;
        }
        return 1.0 - ((LevenshteinDistance) distance).calcText(left, right) / (double) len;
    }

}
