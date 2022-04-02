package com.alibaba.alink.operator.batch.audio;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.audio.ReadAudioToTensorMapper;
import com.alibaba.alink.params.audio.ReadAudioToTensorParams;

@ParamSelectColumnSpec(name="relativeFilePathCol",allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("音频转张量")
public class ReadAudioToTensorBatchOp extends MapBatchOp <ReadAudioToTensorBatchOp>
	implements ReadAudioToTensorParams <ReadAudioToTensorBatchOp> {

	public ReadAudioToTensorBatchOp() {
		this(new Params());
	}

	public ReadAudioToTensorBatchOp(Params params) {
		super(ReadAudioToTensorMapper::new, params);
	}

}
