package com.alibaba.alink.operator.stream.audio;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.audio.ReadAudioToTensorMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.audio.ReadAudioToTensorParams;
@ParamSelectColumnSpec(name="relativeFilePathCol",allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("音频转张量")
public class ReadAudioToTensorStreamOp extends MapStreamOp<ReadAudioToTensorStreamOp>
		implements ReadAudioToTensorParams<ReadAudioToTensorStreamOp> {
	public ReadAudioToTensorStreamOp() {
		this(new Params());
	}

	public ReadAudioToTensorStreamOp(Params params) {
		super(ReadAudioToTensorMapper::new, params);
	}

}
