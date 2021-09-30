package com.alibaba.alink.operator.stream.audio;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.audio.ReadAudioToTensorMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.audio.ReadAudioToTensorParams;

public class ReadAudioToTensorStreamOp extends MapStreamOp<ReadAudioToTensorStreamOp>
		implements ReadAudioToTensorParams<ReadAudioToTensorStreamOp> {
	public ReadAudioToTensorStreamOp() {
		this(new Params());
	}

	public ReadAudioToTensorStreamOp(Params params) {
		super(ReadAudioToTensorMapper::new, params);
	}

}