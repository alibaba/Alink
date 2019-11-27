package com.alibaba.alink.operator.common.nlp.jiebasegment;

public class SegToken {
    public String word;

    public int startOffset;

    public int endOffset;


    public SegToken(String word, int startOffset, int endOffset) {
        this.word = word;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }


    @Override
    public String toString() {
        return "[" + word + ", " + startOffset + ", " + endOffset + "]";
    }

}
