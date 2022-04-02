package com.alibaba.alink.common.pyrunner.fn;

/**
 * A handle to Python user-defined scalar function.
 */
public interface PyScalarFnHandle<PYOUT> extends PyFnHandle {

	/**
	 * Initialize with given config.
	 *
	 * @param fnSpecJson
	 * @param resultType
	 */
	void init(String fnSpecJson, String resultType);

	/**
	 * Call the function with given arguments.
	 *
	 * @param args
	 * @return
	 */
	PYOUT eval(Object[] args);
}
