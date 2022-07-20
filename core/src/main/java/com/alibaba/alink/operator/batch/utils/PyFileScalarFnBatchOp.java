package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.functions.ScalarFunction;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.pyrunner.fn.PyFnFactory;
import com.alibaba.alink.operator.common.utils.UDFHelper;
import com.alibaba.alink.params.udf.PyFileScalarFnParams;
import com.google.gson.JsonObject;

import java.util.Map;

/**
 * Support running scalar functions specified with Python files and the entry function or class.
 */
@Internal
public class PyFileScalarFnBatchOp extends BasePyScalarFnBatchOp <PyFileScalarFnBatchOp>
	implements PyFileScalarFnParams <PyFileScalarFnBatchOp> {

	public PyFileScalarFnBatchOp() {
		this(new Params());
	}

	public PyFileScalarFnBatchOp(Params params) {
		super(params);
	}

	@Override
	protected ScalarFunction getPyScalarFn(String funcName) {
		JsonObject fnSpec = UDFHelper.makeFnSpec(this);
		Map <String, String> runConfig = UDFHelper.makeRunConfig(this);
		return PyFnFactory.makeScalarFn(funcName, getResultType(), fnSpec.toString(), runConfig);
	}
}
