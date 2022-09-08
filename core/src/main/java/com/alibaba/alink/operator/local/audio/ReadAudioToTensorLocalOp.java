package com.alibaba.alink.operator.local.audio;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.audio.ReadAudioToTensorMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.audio.ReadAudioToTensorParams;

@ParamSelectColumnSpec(name = "relativeFilePathCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("音频转张量")
public class ReadAudioToTensorLocalOp extends MapLocalOp <ReadAudioToTensorLocalOp>
	implements ReadAudioToTensorParams <ReadAudioToTensorLocalOp> {

	public ReadAudioToTensorLocalOp() {
		this(new Params());
	}

	public ReadAudioToTensorLocalOp(Params params) {
		super(ReadAudioToTensorMapper::new, params);
	}

}
