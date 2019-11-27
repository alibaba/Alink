package com.alibaba.alink.operator.common.feature;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * The model data of one-hot, which store mapping, vector size and some other things.
 */
public class OneHotModelData {
    public Params meta = new Params();
    public ArrayList<String> data;
    public Map<String, HashMap<String, Integer>> mapping;
    public int otherMapping;
}
