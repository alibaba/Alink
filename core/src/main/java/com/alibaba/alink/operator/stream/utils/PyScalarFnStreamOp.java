package com.alibaba.alink.operator.stream.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.functions.ScalarFunction;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.pyrunner.fn.PyFnFactory;
import com.alibaba.alink.operator.common.utils.UDFHelper;
import com.alibaba.alink.params.udf.BinaryScalarFunctionParams;
import com.google.gson.JsonObject;

import java.util.Map;

@Internal
public class PyScalarFnStreamOp extends BasePyScalarFnStreamOp <PyScalarFnStreamOp> implements
	BinaryScalarFunctionParams <PyScalarFnStreamOp> {

	public PyScalarFnStreamOp() {
		this(new Params());
	}

	public PyScalarFnStreamOp(Params params) {
		super(params);
	}

	@Override
	protected ScalarFunction getPyScalarFn(String funcName) {
		JsonObject fnSpec = UDFHelper.makeFnSpec(this);
		Map <String, String> runConfig = UDFHelper.makeRunConfig(this);
		return PyFnFactory.makeScalarFn(funcName, getResultType(), fnSpec.toString(), runConfig);
	}
}
