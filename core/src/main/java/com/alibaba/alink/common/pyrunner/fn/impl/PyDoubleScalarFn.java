package com.alibaba.alink.common.pyrunner.fn.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.pyrunner.fn.BasePyScalarFn;
import com.alibaba.alink.common.pyrunner.fn.PyScalarFnHandle;
import com.alibaba.alink.common.utils.Functional.SerializableBiFunction;

import java.util.Collections;

@Internal
public class PyDoubleScalarFn extends BasePyScalarFn <Double, PyScalarFnHandle <Double>> {

	public PyDoubleScalarFn(String name, String fnSpecJson) {
		this(name, fnSpecJson, Collections. <String, String>emptyMap()::getOrDefault);
	}

	public PyDoubleScalarFn(String name, String fnSpecJson,
							SerializableBiFunction <String, String, String> runConfigGetter) {
		super(name, fnSpecJson, double.class, runConfigGetter);
	}

	@Override
	public TypeInformation <?> getResultType(Class <?>[] signature) {
		return Types.DOUBLE;
	}

	public double eval(Object... args) {
		return runner.calc(args);
	}
}
