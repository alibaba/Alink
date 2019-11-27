package com.alibaba.alink.operator.common.nlp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.params.nlp.StopWordsRemoverParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;

/**
 * Filter stop words in a document. The default stop words dictionary contains Chinese and English stop words. The
 * Chinese stop words are from https://github.com/uk9921/StopWords, provides by Harbin Institute of Technology. The
 * English stop words are from https://anoncvs.postgresql.org/cvsweb.cgi/pgsql/src/backend/snowball/stopwords/.
 */
public class StopWordsRemoverMapper extends SISOMapper {
    private HashSet<String> stopWordsSet;
    private final boolean caseSensitive;
    private static final Logger LOG = LoggerFactory.getLogger(StopWordsRemoverMapper.class);

    public StopWordsRemoverMapper(TableSchema dataSchema, Params params) {
        super(dataSchema, params);
        this.caseSensitive = this.params.get(StopWordsRemoverParams.CASE_SENSITIVE);
        this.stopWordsSet = new HashSet<>();
        String[] stopWords = this.params.get(StopWordsRemoverParams.STOP_WORDS);
        if (null != stopWords) {
            for(String stopWord : stopWords){
                stopWordsSet.add(caseSensitive ? stopWord : stopWord.toLowerCase());
            }
        }
        loadDefaultStopWords();
    }

    /**
     * Load the default stop words. Include Chinese and English stop words.
     */
    private void loadDefaultStopWords() {
        String defaultStopWordsPath = "/stop.txt";
        InputStream is = this.getClass().getResourceAsStream(defaultStopWordsPath);
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line;
            while ((line = br.readLine()) != null) {
                if (line.length() > 0) {
                    stopWordsSet.add(caseSensitive ? line : line.toLowerCase());
                }
            }
            is.close();
        } catch (IOException e) {
            LOG.warn("Load default stopWords failure!");
        }
    }

    /**
     * Filter the stop words in the document.
     *
     * @param input the document to filter.
     * @return the document filtered.
     */
    @Override
    protected Object mapColumn(Object input) {
        if (null == input) {
            return null;
        }
        String content = (String)input;
        StringBuilder sbd = new StringBuilder();
        String[] tokens = content.split(NLPConstant.WORD_DELIMITER);
        for (String token : tokens) {
            if (stopWordsSet.contains(caseSensitive ? token : token.toLowerCase())) {
                continue;
            }
            sbd.append(token).append(NLPConstant.WORD_DELIMITER);
        }
        return sbd.toString().trim();
    }

    @Override
    protected TypeInformation initOutputColType() {
        return Types.STRING;
    }
}
