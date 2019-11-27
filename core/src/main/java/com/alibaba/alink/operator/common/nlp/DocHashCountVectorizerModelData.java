package com.alibaba.alink.operator.common.nlp;

import java.util.HashMap;

/**
 * Save the data for DocHashIDFVectorizer.
 *
 * Save a HashMap: index(MurMurHash3 value of the word), value(Inverse document frequency of the word).
 */
public class DocHashCountVectorizerModelData {
    public HashMap<Integer, Double> idfMap;
    public int numFeatures;
    public String featureType;
    public double minTF;
}
