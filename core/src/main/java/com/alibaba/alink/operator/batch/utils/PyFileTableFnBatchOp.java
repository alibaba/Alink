package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.functions.TableFunction;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.pyrunner.fn.PyFnFactory;
import com.alibaba.alink.operator.common.utils.UDFHelper;
import com.alibaba.alink.params.udf.PyFileTableFnParams;
import com.google.gson.JsonObject;

import java.util.Map;

@Internal
public class PyFileTableFnBatchOp extends BasePyTableFnBatchOp <PyFileTableFnBatchOp>
	implements PyFileTableFnParams <PyFileTableFnBatchOp> {

	public PyFileTableFnBatchOp() {
		this(new Params());
	}

	public PyFileTableFnBatchOp(Params params) {
		super(params);
	}

	@Override
	protected TableFunction <?> getPyTableFn(String funcName) {
		JsonObject fnSpec = UDFHelper.makeFnSpec(this);
		Map <String, String> runConfig = UDFHelper.makeRunConfig(this);
		return PyFnFactory.makeTableFn(funcName, fnSpec.toString(), getResultTypes(), runConfig);
	}
}
