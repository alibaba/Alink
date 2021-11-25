package com.alibaba.alink.operator.common.recommendation;

import com.alibaba.alink.common.utils.AlinkSerializable;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;

public class SwingResData implements AlinkSerializable {
    private Object[] object;
    private Float[] score;
    private String itemCol;
    public SwingResData() {
    }

    public SwingResData(Object[] object, Float[] score, String itemCol) {
        this.object = object;
        this.score = score;
        this.itemCol = itemCol;
    }

    public void setObject(Object[] object) {
        this.object = object;
    }

    public void setScore(Float[] score) {
        this.score = score;
    }

    public Object[] getObject() {
        return object;
    }

    public Float[] getScore() {
        return score;
    }

    public String returnTopNData(int topN) {
        int thisSize = Math.min(object.length, topN);
        List<Object> resItems = new ArrayList<>(thisSize);
        List<Double> resSimilarity = new ArrayList<>(thisSize);
        for (int i = 0; i < thisSize; i++) {
            resItems.add(object[i]);
            resSimilarity.add((double) score[i]);
        }

        return KObjectUtil.serializeRecomm(
            itemCol,
            resItems,
            ImmutableMap.of("score", resSimilarity));
    }
}
