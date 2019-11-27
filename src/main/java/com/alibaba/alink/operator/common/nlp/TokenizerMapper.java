package com.alibaba.alink.operator.common.nlp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.mapper.SISOMapper;

/**
 * Transform all words into lower case, and split it by white space.
 */
public class TokenizerMapper extends SISOMapper {
	private static final String SPLIT_DELIMITER = "\\s";

	public TokenizerMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	@Override
	public TypeInformation initOutputColType() {
		return Types.STRING;
	}

	@Override
	public Object mapColumn(Object input) {
		if (null == input) {
			return null;
		}
		String content = ((String) input).toLowerCase();
		StringBuilder builder = new StringBuilder();
		String[] tokens = content.split(SPLIT_DELIMITER);
		for(String token : tokens) {
			builder.append(token).append(NLPConstant.WORD_DELIMITER);
		}
		return builder.toString().trim();
	}
}
