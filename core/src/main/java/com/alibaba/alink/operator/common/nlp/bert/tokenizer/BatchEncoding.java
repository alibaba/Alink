package com.alibaba.alink.operator.common.nlp.bert.tokenizer;

import java.util.HashMap;

/**
 * A Java implementation of HuggingFace `BatchEncoding` with following changes:
 * <p>
 * 1. only store tokenization results of batch examples. For tokenization result of single example, check {@link
 * SingleEncoding}.
 * <p>
 * 2. all stored values are 2-d int array, including length.
 */
public class BatchEncoding extends HashMap <EncodingKeys, int[][]> {
}
