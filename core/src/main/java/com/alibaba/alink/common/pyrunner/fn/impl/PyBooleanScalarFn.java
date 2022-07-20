package com.alibaba.alink.common.pyrunner.fn.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.pyrunner.fn.BasePyScalarFn;
import com.alibaba.alink.common.pyrunner.fn.PyScalarFnHandle;

import java.util.Collections;
import java.util.Map;

@Internal
public class PyBooleanScalarFn extends BasePyScalarFn <Boolean, PyScalarFnHandle <Boolean>> {

	public PyBooleanScalarFn(String name, String fnSpecJson) {
		this(name, fnSpecJson, Collections.emptyMap());
	}

	public PyBooleanScalarFn(String name, String fnSpecJson, Map <String, String> runConfig) {
		super(name, fnSpecJson, boolean.class, runConfig);
	}

	@Override
	public TypeInformation <?> getResultType(Class <?>[] signature) {
		return Types.BOOLEAN;
	}

	public boolean eval(Object... args) {
		return runner.calc(args);
	}
}
