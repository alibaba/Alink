package com.alibaba.alink.pipeline.audio;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.audio.ReadAudioToTensorMapper;
import com.alibaba.alink.params.audio.ReadAudioToTensorParams;
import com.alibaba.alink.pipeline.MapTransformer;

@NameCn("音频转张量")
public class ReadAudioToTensor extends MapTransformer <ReadAudioToTensor>
	implements ReadAudioToTensorParams <ReadAudioToTensor> {

	public ReadAudioToTensor() {
		this(null);
	}

	public ReadAudioToTensor(Params params) {
		super(ReadAudioToTensorMapper::new, params);
	}
}
