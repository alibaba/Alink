package com.alibaba.alink.common.pyrunner;

/**
 * Indicate the Java object of this type is a handle to a Python object.
 * Functions in the Python object can be called through the handle.
 * <p>
 * The corresponding Python class should be tagged with full name of this interface or its sub-interfaces,
 * see
 * <a href="https://www.py4j.org/advanced_topics.html#implementing-java-interfaces-from-python-callback">Py4J documents</a>
 * for more details.
 */
public interface PyObjHandle {
}
