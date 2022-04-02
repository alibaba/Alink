package com.alibaba.alink.common.pyrunner.fn.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.pyrunner.fn.BasePyScalarFn;
import com.alibaba.alink.common.pyrunner.fn.PyScalarFnHandle;
import com.alibaba.alink.common.utils.Functional.SerializableBiFunction;

import java.util.Collections;

@Internal
public class PyStringScalarFn extends BasePyScalarFn <String, PyScalarFnHandle <String>> {

	public PyStringScalarFn(String name, String fnSpecJson) {
		this(name, fnSpecJson, Collections. <String, String>emptyMap()::getOrDefault);
	}

	public PyStringScalarFn(String name, String fnSpecJson,
							SerializableBiFunction <String, String, String> runConfigGetter) {
		super(name, fnSpecJson, String.class, runConfigGetter);
	}

	@Override
	public TypeInformation <?> getResultType(Class <?>[] signature) {
		return Types.STRING;
	}

	public String eval(Object... args) {
		return runner.calc(args);
	}
}
