package com.alibaba.alink.operator.common.nlp;

import java.util.List;

/**
 * Save the data for DocHashIDFVectorizer.
 *
 * Save a HashMap: index(MurMurHash3 value of the word), value(Inverse document frequency of the word).
 */
public class DocCountVectorizerModelData {
    public List<String> list;
    public String featureType;
    public double minTF;
}
