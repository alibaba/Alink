package com.alibaba.alink.operator.common.recommendation;

import java.util.List;
import java.util.Map;

/**
 * The ALS model data.
 */
public class AlsModelData {
    public float[][] userFactors;
    public float[][] itemFactors;
    public Map<Long, Integer> userIdMap;
    public Map<Long, Integer> itemIdMap;
    public List<Long> userIds;
    public List<Long> itemIds;
}
