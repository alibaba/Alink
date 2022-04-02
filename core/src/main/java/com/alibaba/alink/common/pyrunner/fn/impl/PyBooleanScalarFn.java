package com.alibaba.alink.common.pyrunner.fn.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.pyrunner.fn.BasePyScalarFn;
import com.alibaba.alink.common.pyrunner.fn.PyScalarFnHandle;
import com.alibaba.alink.common.utils.Functional.SerializableBiFunction;

import java.util.Collections;

@Internal
public class PyBooleanScalarFn extends BasePyScalarFn <Boolean, PyScalarFnHandle <Boolean>> {

	public PyBooleanScalarFn(String name, String fnSpecJson) {
		this(name, fnSpecJson, Collections. <String, String>emptyMap()::getOrDefault);
	}

	public PyBooleanScalarFn(String name, String fnSpecJson,
							 SerializableBiFunction <String, String, String> runConfigGetter) {
		super(name, fnSpecJson, boolean.class, runConfigGetter);
	}

	@Override
	public TypeInformation <?> getResultType(Class <?>[] signature) {
		return Types.BOOLEAN;
	}

	public boolean eval(Object... args) {
		return runner.calc(args);
	}
}
