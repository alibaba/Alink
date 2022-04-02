package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.nlp.bert.BertTextEmbeddingMapper;
import com.alibaba.alink.params.tensorflow.bert.BertTextEmbeddingParams;
import com.alibaba.alink.pipeline.MapTransformer;

@NameCn("Bert文本嵌入")
public class BertTextEmbedding extends MapTransformer<BertTextEmbedding>
    implements BertTextEmbeddingParams<BertTextEmbedding> {

    public BertTextEmbedding() {
        this(new Params());
    }

    public BertTextEmbedding(Params params) {
        super(BertTextEmbeddingMapper::new, params);
    }
}

