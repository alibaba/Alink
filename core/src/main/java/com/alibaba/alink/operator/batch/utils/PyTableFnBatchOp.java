package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.functions.TableFunction;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.pyrunner.fn.PyFnFactory;
import com.alibaba.alink.operator.common.utils.UDFHelper;
import com.alibaba.alink.params.udf.BinaryTableFunctionParams;
import com.google.gson.JsonObject;

import java.util.Map;

@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.DATA, desc = PortDesc.OUTPUT_RESULT)})
@Internal
public class PyTableFnBatchOp extends BasePyTableFnBatchOp <PyTableFnBatchOp>
	implements BinaryTableFunctionParams <PyTableFnBatchOp> {

	public PyTableFnBatchOp() {
		super(null);
	}

	public PyTableFnBatchOp(Params params) {
		super(params);
	}

	@Override
	protected TableFunction <?> getPyTableFn(String funcName) {
		JsonObject fnSpec = UDFHelper.makeFnSpec(this);
		Map <String, String> runConfig = UDFHelper.makeRunConfig(this);
		return PyFnFactory.makeTableFn(funcName, fnSpec.toString(), getResultTypes(), runConfig);
	}
}
