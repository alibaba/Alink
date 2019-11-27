package com.alibaba.alink.operator.common.nlp;

import com.alibaba.alink.operator.common.nlp.jiebasegment.JiebaSegmenter;
import com.alibaba.alink.operator.common.nlp.jiebasegment.SegToken;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.params.nlp.SegmentParams;

import java.util.List;

/**
 * Segment Chinese document into words.
 */
public class SegmentMapper extends SISOMapper {
	private JiebaSegmenter segmentor;

	public SegmentMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		segmentor = new JiebaSegmenter();
		String[] userDefinedDict = this.params.get(SegmentParams.USER_DEFINED_DICT);
		if (null != userDefinedDict) {
			for(String word : userDefinedDict){
				segmentor.addUserWord2Dict(word);
			}
		}
	}

	@Override
	protected Object mapColumn(Object input) {
		if (null == input) {
			return null;
		}
		String content = (String) input;
		List <SegToken> tokens;
		tokens = segmentor.process(content, JiebaSegmenter.SegMode.SEARCH);
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < tokens.size(); i++) {
			sbd.append(tokens.get(i).word).append(NLPConstant.WORD_DELIMITER);
		}
		return sbd.toString().trim();
	}

	@Override
	protected TypeInformation initOutputColType() {
		return Types.STRING;
	}
}
