package com.alibaba.alink.operator.batch.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.vector.VectorFunctionMapper;
import com.alibaba.alink.params.dataproc.vector.VectorFunctionParams;

/**
 * Find maxValue / minValue / maxValue index / minValue index in Vector
 * Vector can be sparse vector or dense vector.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = {TypeCollections.VECTOR_TYPES})
@NameCn("向量函数")
@NameEn("Vector Function")
public final class VectorFunctionBatchOp extends MapBatchOp <VectorFunctionBatchOp>
	implements VectorFunctionParams <VectorFunctionBatchOp> {

	private static final long serialVersionUID = -5580521679868956131L;

	public VectorFunctionBatchOp() {
		this(null);
	}

	public VectorFunctionBatchOp(Params params) {
		super(VectorFunctionMapper::new, params);
	}
}
