package com.alibaba.alink.common.linalg;

import java.io.Serializable;

/**
 * The Vector class defines some common methods for both DenseVector and
 * SparseVector.
 */
public abstract class Vector implements Serializable {
    /**
     * Get the size of the vector.
     */
    public abstract int size();

    /**
     * Get the i-th element of the vector.
     */
    public abstract double get(int i);

    /**
     * Set the i-th element of the vector to value "val".
     */
    public abstract void set(int i, double val);

    /**
     * Add the i-th element of the vector by value "val".
     */
    public abstract void add(int i, double val);

    /**
     * Return the L1 norm of the vector.
     */
    public abstract double normL1();

    /**
     * Return the Inf norm of the vector.
     */
    public abstract double normInf();

    /**
     * Return the L2 norm of the vector.
     */
    public abstract double normL2();

    /**
     * Return the square of L2 norm of the vector.
     */
    public abstract double normL2Square();

    /**
     * Scale the vector by value "v" and create a new vector to store the result.
     */
    public abstract Vector scale(double v);

    /**
     * Scale the vector by value "v".
     */
    public abstract void scaleEqual(double v);

    /**
     * Normalize the vector.
     */
    public abstract void normalizeEqual(double p);

    /**
     * Standardize the vector.
     */
    public abstract void standardizeEqual(double mean, double stdvar);

    /**
     * Create a new vector by adding an element to the head of the vector.
     */
    public abstract Vector prefix(double v);

    /**
     * Create a new vector by adding an element to the end of the vector.
     */
    public abstract Vector append(double v);

    /**
     * Create a new vector by plussing another vector.
     */
    public abstract Vector plus(Vector vec);

    /**
     * Create a new vector by subtracting  another vector.
     */
    public abstract Vector minus(Vector vec);

    /**
     * Compute the dot product with another vector.
     */
    public abstract double dot(Vector vec);

    /**
     * Get the iterator of the vector.
     */
    public abstract VectorIterator iterator();

    /**
     * Slice the vector.
     */
    public abstract Vector slice(int[] indexes);

    /**
     * Compute the outer product with itself.
     *
     * @return The outer product matrix.
     */
    public abstract DenseMatrix outer();


    /**
     * Parse either a {@link SparseVector} or a {@link DenseVector} from a formatted string.
     *
     * <p>The format of a dense vector is comma separated values such as "1 2 3 4".
     * The format of a sparse vector is comma separated index-value pairs, such as "0:1 2:3 3:4".
     * If the sparse vector has determined vector size, the size is prepended to the head. For example,
     * the string "$4$0:1 2:3 3:4" represents a sparse vector with size 4.
     *
     * @param str A formatted string representing a vector.
     * @return The parsed vector.
     */
    //public static Vector getVector(String str) {
    //    boolean isSparse = org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(str)
    //        || StringUtils.indexOf(str, SparseVector.INDEX_VALUE_DELIMITER) != -1
    //        || StringUtils.indexOf(str, SparseVector.HEADER_DELIMITER) != -1;
    //    if (isSparse) {
    //        return SparseVector.parseDense(str);
    //    } else {
    //        return DenseVector.parseDense(str);
    //    }
    //}
}
