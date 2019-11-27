package com.alibaba.alink.operator.common.nlp;

/**
 * Common used delimiters for NLP.
 */
public class NLPConstant {
    /**
     * Word delimiter used for split the documents, concat the processed words.
     *
     * <p>Besides Segment, Tokenizer and RegTokenizer, all the document inputs are split with this delimiter.
     *
     * <p></p>And this delimiter is used in all cases that the output of the algorithm is concat with processed words.
     */
    public static final String WORD_DELIMITER = " ";
}
