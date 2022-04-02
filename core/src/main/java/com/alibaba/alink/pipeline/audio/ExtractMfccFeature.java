package com.alibaba.alink.pipeline.audio;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.audio.ExtractMfccFeatureMapper;
import com.alibaba.alink.params.audio.ExtractMfccFeatureParams;
import com.alibaba.alink.pipeline.MapTransformer;

@NameCn("MFCC特征提取")
public class ExtractMfccFeature extends MapTransformer<ExtractMfccFeature>
    implements ExtractMfccFeatureParams<ExtractMfccFeature> {

    public ExtractMfccFeature() {
        this(null);
    }

    public ExtractMfccFeature(Params params) {
        super(ExtractMfccFeatureMapper::new, params);
    }
}
