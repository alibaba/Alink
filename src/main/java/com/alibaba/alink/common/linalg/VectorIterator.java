package com.alibaba.alink.common.linalg;

import java.io.Serializable;
import java.util.Iterator;

/**
 * An iterator over the elements of a vector.
 *
 * <p>Usage:
 *
 * <code>
 * Vector vector = ...;
 * VectorIterator iterator = vector.iterator();
 *
 * while(iterator.hasNext()) {
 * int index = iterator.getIndex();
 * double value = iterator.getValue();
 * iterator.next();
 * }
 * </code>
 */
public interface VectorIterator extends Serializable {

	/**
	 * Returns {@code true} if the iteration has more elements.
	 * Otherwise, {@code false} will be returned.
	 *
	 * @return {@code true} if the iteration has more elements
	 */
	boolean hasNext();

	/**
	 * Trigger the cursor points to the next element of the vector.
	 *
	 * <p>The {@link #getIndex()} while returns the index of the
	 * element which the cursor points.
	 * The {@link #getValue()} ()} while returns the value of
	 * the element which the cursor points.
	 *
	 * <p>The difference to the {@link Iterator#next()} is that this
	 * can avoid the return of boxed type.
	 */
	void next();

	/**
	 * Returns the index of the element which the cursor points.
	 *
	 * @returnthe the index of the element which the cursor points.
	 */
	int getIndex();

	/**
	 * Returns the value of the element which the cursor points.
	 *
	 * @returnthe the value of the element which the cursor points.
	 */
	double getValue();
}
