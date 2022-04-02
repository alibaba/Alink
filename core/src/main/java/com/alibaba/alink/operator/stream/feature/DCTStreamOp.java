package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.common.feature.DCTMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.feature.DCTParams;

/**
 * Discrete Cosine Transform(DCT) transforms a real-valued sequence in the time domain into another real-valued sequence
 * with same length in the frequency domain.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "selectedCol")
@NameCn("离散余弦变换")
public class DCTStreamOp extends MapStreamOp <DCTStreamOp>
	implements DCTParams <DCTStreamOp> {

	private static final long serialVersionUID = -3314453498652632314L;

	public DCTStreamOp() {
		this(null);
	}

	public DCTStreamOp(Params params) {
		super(DCTMapper::new, params);
	}
}
