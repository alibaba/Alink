package com.alibaba.alink.operator.common.clustering.lda;

import java.io.Serializable;

/**
 * This class denotes all words and their topics of a document.
 * A word may be duplicated, and it will have indices in wordIdxs and may have different topicIdxs.
 */
public class Document implements Serializable {

    //the word list.
    private int[] wordIdxs;
    //the topic of all words.
    private int[] topicIdxs;

    public Document(int length) {
        this.wordIdxs = new int[length];
        this.topicIdxs = new int[length];
    }

    void setWordIdxs(int index, int value) {
        this.wordIdxs[index] = value;
    }

    void setTopicIdxs(int index, int value) {
        this.topicIdxs[index] = value;
    }

    int getWordIdxs(int index) {
        return this.wordIdxs[index];
    }

    int getTopicIdxs(int index) {
        return this.topicIdxs[index];
    }

    int getLength() {
        return this.wordIdxs.length;
    }
}
