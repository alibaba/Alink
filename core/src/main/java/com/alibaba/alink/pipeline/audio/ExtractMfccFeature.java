package com.alibaba.alink.pipeline.audio;

import com.alibaba.alink.operator.common.audio.ExtractMfccFeatureMapper;
import com.alibaba.alink.params.audio.ExtractMfccFeatureParams;
import com.alibaba.alink.pipeline.MapTransformer;
import org.apache.flink.ml.api.misc.param.Params;

public class ExtractMfccFeature extends MapTransformer<ExtractMfccFeature>
    implements ExtractMfccFeatureParams<ExtractMfccFeature> {

    public ExtractMfccFeature() {
        this(null);
    }

    public ExtractMfccFeature(Params params) {
        super(ExtractMfccFeatureMapper::new, params);
    }
}