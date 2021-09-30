package com.alibaba.alink.operator.common.nlp.bert.tokenizer;

public enum EncodingKeys {
	INPUT_IDS_KEY("input_ids"),
	TOKEN_TYPE_IDS_KEY("token_type_ids"),
	ATTENTION_MASK_KEY("attention_mask"),
	OVERFLOWING_TOKENS_KEY("overflowing_tokens"),
	NUM_TRUNCATED_TOKENS_KEY("num_truncated_tokens"),
	SPECIAL_TOKENS_MASK_KEY("special_tokens_mask"),
	LENGTH_KEY("length"),
	NUM_TRUNCATED_TOKENS("num_truncated_tokens");

	public final String label;

	EncodingKeys(String label) {
		this.label = label;
	}
}
