package com.alibaba.alink.common.pyrunner.fn;

import com.alibaba.alink.common.pyrunner.PyCalcRunner;
import com.alibaba.alink.common.utils.Functional.SerializableBiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

public class PyScalarFnRunner<OUT, HANDLE extends PyScalarFnHandle <OUT>> extends PyCalcRunner <Object[], OUT, HANDLE> {

	private final static Logger LOG = LoggerFactory.getLogger(PyScalarFnRunner.class);

	public static final String PY_CLASS_NAME = "alink.fn.PyScalarFn";

	private String fnSpecJson;
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
	public void preOpenBridgeHook(Path workDir) {
		super.preOpenBridgeHook(workDir);
		fnSpecJson = PyFnUtils.downloadFilePaths(fnSpecJson, workDir);
	}

	@Override
	public void open() {
		super.open();
		handle.init(fnSpecJson, returnType.getCanonicalName());
	}

	@Override
	public OUT calc(Object[] objects) {
		long start = System.currentTimeMillis();
		objects = Arrays.stream(objects)
			.map(DataConversionUtils::javaToPy)
			.toArray(Object[]::new);
		OUT result = handle.eval(objects);
		long end = System.currentTimeMillis();
		LOG.info("Elapsed time: {}", end - start);
		return result;
	}
}
