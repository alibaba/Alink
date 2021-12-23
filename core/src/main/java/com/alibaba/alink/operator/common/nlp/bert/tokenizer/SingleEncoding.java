package com.alibaba.alink.operator.common.nlp.bert.tokenizer;

import java.util.HashMap;

/**
 * A Java implementation of HuggingFace `BatchEncoding` with following changes:
 * <p>
 * 1. only store tokenization results for a single example.
 * <p>
 * 2. all stored values are 1-d int array, including length.
 */
public class SingleEncoding extends HashMap <EncodingKeys, int[]> {
}
