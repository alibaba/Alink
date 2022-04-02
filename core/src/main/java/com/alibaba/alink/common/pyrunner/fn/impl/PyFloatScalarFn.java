package com.alibaba.alink.common.pyrunner.fn.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.pyrunner.fn.BasePyScalarFn;
import com.alibaba.alink.common.pyrunner.fn.PyScalarFnHandle;
import com.alibaba.alink.common.utils.Functional.SerializableBiFunction;

import java.util.Collections;

@Internal
public class PyFloatScalarFn extends BasePyScalarFn <Float, PyScalarFnHandle <Float>> {

	public PyFloatScalarFn(String name, String fnSpecJson) {
		this(name, fnSpecJson, Collections. <String, String>emptyMap()::getOrDefault);
	}

	public PyFloatScalarFn(String name, String fnSpecJson,
						   SerializableBiFunction <String, String, String> runConfigGetter) {
		super(name, fnSpecJson, float.class, runConfigGetter);
	}

	@Override
	public TypeInformation <?> getResultType(Class <?>[] signature) {
		return Types.FLOAT;
	}

	public float eval(Object... args) {
		return runner.calc(args);
	}
}
