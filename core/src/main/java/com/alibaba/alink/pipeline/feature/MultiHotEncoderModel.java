package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.feature.MultiHotModelMapper;
import com.alibaba.alink.params.feature.MultiHotPredictParams;
import com.alibaba.alink.pipeline.MapModel;

@NameCn("多热编码模型")
public class MultiHotEncoderModel extends MapModel<MultiHotEncoderModel>
    implements MultiHotPredictParams<MultiHotEncoderModel> {

	private static final long serialVersionUID = -901650815591602025L;

	public MultiHotEncoderModel() {
        this(new Params());
    }

    public MultiHotEncoderModel(Params params) {
        super(MultiHotModelMapper::new, params);
    }

}
