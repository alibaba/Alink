package com.alibaba.alink.common.pyrunner.fn;

import com.alibaba.alink.common.pyrunner.PyCalcRunner;
import com.alibaba.alink.common.utils.Functional.SerializableBiFunction;

import java.util.Arrays;
import java.util.Collections;

public class PyScalarFnRunner<OUT, HANDLE extends PyScalarFnHandle <OUT>> extends PyCalcRunner <Object[], OUT, HANDLE> {

	public static final String PY_CLASS_NAME = "alink.fn.PyScalarFn";

	private final String fnSpecJson;
	private final Class <?> returnType;

	public PyScalarFnRunner(String fnSpecJson, Class <?> returnType) {
		this(fnSpecJson, returnType, Collections. <String, String>emptyMap()::getOrDefault);
	}

	public PyScalarFnRunner(String fnSpecJson, Class <?> returnType,
							SerializableBiFunction <String, String, String> runConfigGetter) {
		super(PY_CLASS_NAME, runConfigGetter);
		this.fnSpecJson = fnSpecJson;
		this.returnType = returnType;
	}

	@Override
	public void open() {
		super.open();
		handle.init(fnSpecJson, returnType.getCanonicalName());
	}

	@Override
	public OUT calc(Object[] objects) {
		objects = Arrays.stream(objects)
			.map(DataConversionUtils::javaToPy)
			.toArray(Object[]::new);
		return handle.eval(objects);
	}
}
