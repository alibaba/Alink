package com.alibaba.alink.operator.local.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.vector.VectorFunctionMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.dataproc.vector.VectorFunctionParams;

/**
 * Find maxValue / minValue / maxValue index / minValue index in Vector
 * Vector can be sparse vector or dense vector.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = {TypeCollections.VECTOR_TYPES})
@NameCn("向量函数")
public final class VectorFunctionLocalOp extends MapLocalOp <VectorFunctionLocalOp>
	implements VectorFunctionParams <VectorFunctionLocalOp> {

	public VectorFunctionLocalOp() {
		this(null);
	}

	public VectorFunctionLocalOp(Params params) {
		super(VectorFunctionMapper::new, params);
	}
}
