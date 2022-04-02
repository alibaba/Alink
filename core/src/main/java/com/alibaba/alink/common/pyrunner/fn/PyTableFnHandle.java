package com.alibaba.alink.common.pyrunner.fn;

import com.alibaba.alink.common.pyrunner.fn.PyTableFn.PyCollector;

/**
 * A handle to Python user-defined table function.
 */
public interface PyTableFnHandle extends PyFnHandle {

	/**
	 * Initialize with given config.
	 *
	 * @param pyCollector
	 * @param fnSpecJson
	 * @param resultTypes
	 */
	void init(PyCollector pyCollector, String fnSpecJson, String[] resultTypes);

	/**
	 * Call the function with given arguments.
	 *
	 * @param args
	 * @return
	 */
	void eval(Object[] args);
}
