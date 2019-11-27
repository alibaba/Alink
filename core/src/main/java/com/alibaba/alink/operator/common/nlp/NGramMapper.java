package com.alibaba.alink.operator.common.nlp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.params.nlp.NGramParams;

/**
 * Transform a document into a new document composed of all its nGrams. The document is split into an array of words by
 * a word delimiter(default space). Through sliding the word array, we get all nGrams and each nGram is connected with a
 * "_" character. All the nGrams are joined together with outputDelimiter in the new document.
 */
public class NGramMapper extends SISOMapper {
    private static final String CONNECTOR_CHARACTER = "_";

    private final int n;

    public NGramMapper(TableSchema dataSchema, Params params) {
        super(dataSchema, params);
        this.n = this.params.get(NGramParams.N);
        if (this.n <= 0) {
            throw new IllegalArgumentException("N must be positive!");
        }
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
        String content = (String)input;
        String[] tokens = content.split(NLPConstant.WORD_DELIMITER);
        StringBuilder sbd = new StringBuilder();
        for (int i = 0; i < 1 + tokens.length - n; i++) {
            for (int j = 0; j < n; j++) {
                if (j > 0) {
                    sbd.append(CONNECTOR_CHARACTER);
                }
                sbd.append(tokens[i + j]);
            }
            sbd.append(NLPConstant.WORD_DELIMITER);
        }
        return sbd.toString().trim();
    }
}
