package com.alibaba.alink.common.pyrunner.fn;

import com.alibaba.alink.common.pyrunner.PyCalcRunner;
import com.alibaba.alink.common.pyrunner.fn.PyTableFn.PyCollector;
import com.alibaba.alink.common.utils.Functional.SerializableBiFunction;

import java.util.Arrays;

public class PyTableFnRunner extends PyCalcRunner <Object[], Void, PyTableFnHandle> {

	public static final String PY_CLASS_NAME = "alink.fn.PyTableFn";

	private final PyCollector pyCollector;
	private final String fnSpecJson;
	private final String[] resultTypeStrs;

	public PyTableFnRunner(PyCollector pyCollector, String fnSpecJson, String[] resultTypeStrs,
						   SerializableBiFunction <String, String, String> runConfigGetter) {
		super(PY_CLASS_NAME, runConfigGetter);
		this.pyCollector = pyCollector;
		this.fnSpecJson = fnSpecJson;
		this.resultTypeStrs = resultTypeStrs;
	}

	@Override
	public void open() {
		super.open();
		handle.init(pyCollector, fnSpecJson, resultTypeStrs);
	}

	@Override
	public Void calc(Object[] objects) {
		objects = Arrays.stream(objects)
			.map(DataConversionUtils::javaToPy)
			.toArray(Object[]::new);
		handle.eval(objects);
		return null;
	}
}
