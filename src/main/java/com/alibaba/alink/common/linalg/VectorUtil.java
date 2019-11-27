package com.alibaba.alink.common.linalg;

import org.apache.commons.lang3.StringUtils;

/**
 * Utility class for the operations on {@link Vector} and its subclasses.
 */
public class VectorUtil {
	/**
	 * Delimiter between elements.
	 */
	private static final char ELEMENT_DELIMITER = ' ';
	/**
	 * Delimiter between vector size and vector data.
	 */
	private static final char HEADER_DELIMITER = '$';
	/**
	 * Delimiter between index and value.
	 */
	private static final char INDEX_VALUE_DELIMITER = ':';

	/**
	 * Parse either a {@link SparseVector} or a {@link DenseVector} from a formatted string.
	 *
	 * <p>The format of a dense vector is space separated values such as "1 2 3 4".
	 * The format of a sparse vector is space separated index-value pairs, such as "0:1 2:3 3:4".
	 * If the sparse vector has determined vector size, the size is prepended to the head. For example,
	 * the string "$4$0:1 2:3 3:4" represents a sparse vector with size 4.
	 *
	 * @param str A formatted string representing a vector.
	 * @return The parsed vector.
	 */
	private static Vector parse(String str) {
		boolean isSparse = org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(str)
			|| StringUtils.indexOf(str, INDEX_VALUE_DELIMITER) != -1
			|| StringUtils.indexOf(str, HEADER_DELIMITER) != -1;
		if (isSparse) {
			return parseSparse(str);
		} else {
			return parseDense(str);
		}
	}

	/**
	 * Parse the dense vector from a formatted string.
	 *
	 * <p>The format of a dense vector is space separated values such as "1 2 3 4".
	 *
	 * @param str A string of space separated values.
	 * @return The parsed vector.
	 */
	public static DenseVector parseDense(String str) {
		if (org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(str)) {
			return new DenseVector();
		}

		int len = str.length();

		int inDataBuffPos = 0;
		boolean isInBuff = false;

		for (int i = 0; i < len; ++i) {
			char c = str.charAt(i);

			if (c == ELEMENT_DELIMITER
				// to be compatible with previous delimiter
				|| c == ',') {
				if (isInBuff) {
					inDataBuffPos++;
				}

				isInBuff = false;
			} else {
				isInBuff = true;
			}
		}

		if (isInBuff) {
			inDataBuffPos++;
		}

		double[] data = new double[inDataBuffPos];
		int lastestInCharBuffPos = 0;

		inDataBuffPos = 0;
		isInBuff = false;

		for (int i = 0; i < len; ++i) {
			char c = str.charAt(i);

			if (c == ELEMENT_DELIMITER
				// to be compatible with previous delimiter
				|| c == ',') {
				if (isInBuff) {
					data[inDataBuffPos++] = Double.parseDouble(
						StringUtils.substring(str, lastestInCharBuffPos, i).trim()
					);

					lastestInCharBuffPos = i + 1;
				}

				isInBuff = false;
			} else {
				isInBuff = true;
			}
		}

		if (isInBuff) {
			data[inDataBuffPos] = Double.valueOf(
				StringUtils.substring(str, lastestInCharBuffPos).trim()
			);
		}

		return new DenseVector(data);
	}

	/**
	 * Parse the sparse vector from a formatted string.
	 *
	 * <p>The format of a sparse vector is space separated index-value pairs, such as "0:1 2:3 3:4".
	 * If the sparse vector has determined vector size, the size is prepended to the head. For example,
	 * the string "$4$0:1 2:3 3:4" represents a sparse vector with size 4.
	 *
	 * @throws IllegalArgumentException If the string is of invalid format.
	 */
	public static SparseVector parseSparse(String str) {
		try {
			if (org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(str)) {
				return new SparseVector();
			}

			int n = -1;
			int firstDollarPos = str.indexOf(HEADER_DELIMITER);
			int lastDollarPos = -1;
			if (firstDollarPos >= 0) {
				lastDollarPos = StringUtils.lastIndexOf(str, HEADER_DELIMITER);
				String sizeStr = StringUtils.substring(str, firstDollarPos + 1, lastDollarPos);
				n = Integer.valueOf(sizeStr);
				if (lastDollarPos == str.length() - 1) {
					return new SparseVector(n);
				}
			}

			int numValues = StringUtils.countMatches(str, String.valueOf(INDEX_VALUE_DELIMITER));
			double[] data = new double[numValues];
			int[] indices = new int[numValues];
			int startPos = lastDollarPos + 1;
			int endPos;
			for (int i = 0; i < numValues; i++) {
				int colonPos = StringUtils.indexOf(str, INDEX_VALUE_DELIMITER, startPos);
				if (colonPos < 0) {
					throw new IllegalArgumentException("Format error.");
				}
				endPos = StringUtils.indexOf(str, ELEMENT_DELIMITER, colonPos);

				//to be compatible with previous delimiter
				if (endPos == -1) {
					endPos = StringUtils.indexOf(str, ",", colonPos);
				}

				if (endPos == -1) {
					endPos = str.length();
				}
				indices[i] = Integer.valueOf(str.substring(startPos, colonPos).trim());
				data[i] = Double.valueOf(str.substring(colonPos + 1, endPos).trim());
				startPos = endPos + 1;
			}
			return new SparseVector(n, indices, data);
		} catch (Exception e) {
			throw new IllegalArgumentException(
				String.format("Fail to getVector sparse vector from string: \"%s\".", str),
				e);
		}
	}

	/**
	 * Serialize the vector to a string.
	 *
	 * @see #toString(DenseVector)
	 * @see #toString(SparseVector)
	 */
	public static String toString(Vector vector) {
		if (vector instanceof SparseVector) {
			return toString((SparseVector) vector);
		}
		return toString((DenseVector) vector);
	}

	/**
	 * Serialize the SparseVector to string.
	 *
	 * <p>The format of the returned is described at {@link #parseSparse(String)}
	 */
	public static String toString(SparseVector sparseVector) {
		StringBuilder sbd = new StringBuilder();
		if (sparseVector.n > 0) {
			sbd.append(HEADER_DELIMITER);
			sbd.append(sparseVector.n);
			sbd.append(HEADER_DELIMITER);
		}
		if (null != sparseVector.indices) {
			for (int i = 0; i < sparseVector.indices.length; i++) {
				sbd.append(sparseVector.indices[i]);
				sbd.append(INDEX_VALUE_DELIMITER);
				sbd.append(sparseVector.values[i]);
				if (i < sparseVector.indices.length - 1) {
					sbd.append(ELEMENT_DELIMITER);
				}
			}
		}

		return sbd.toString();
	}

	/**
	 * Serialize the DenseVector to String.
	 *
	 * <p>The format of the returned is described at {@link #parseDense(String)}
	 */
	public static String toString(DenseVector denseVector) {
		StringBuilder sbd = new StringBuilder();

		for (int i = 0; i < denseVector.data.length; i++) {
			sbd.append(denseVector.data[i]);
			if (i < denseVector.data.length - 1) {
				sbd.append(ELEMENT_DELIMITER);
			}
		}
		return sbd.toString();
	}

	/**
	 * Get the vector from {@link Vector}, String or Number.
	 *
	 * <ul>
	 * <li>when the obj is Vector, it will return itself</li>
	 * <li>when the obj is String, it will be parsed to Vector using {@link #parse(String)}</li>
	 * <li>when the obj is Number, it will construct a new {@link DenseVector} which contains the obj.</li>
	 * </ul>
	 *
	 * @param obj the object casted to vector.
	 * @return The casted vector
	 */
	public static Vector getVector(Object obj) {
		if (null == obj) {
			return null;
		}
		if (obj instanceof Vector) {
			return (Vector) obj;
		} else if (obj instanceof String) {
			return parse((String) obj);
		} else if (obj instanceof Number) {
			return new DenseVector(new double[] {((Number) obj).doubleValue()});
		} else {
			throw new IllegalArgumentException("Can not get the vector from " + obj.toString());
		}
	}

	/**
	 * Get the vector from {@link Vector}, String or Number.
	 * The difference to the {@link #getVector(Object)} is
	 * that it will translate the SparseVector to DenseVector
	 * when the obj is a SparseVector.
	 *
	 * @param obj the object casted to vector.
	 * @return The casted DenseVector
	 */
	public static DenseVector getDenseVector(Object obj) {
		Vector vec = getVector(obj);
		if (null == vec) {
			return null;
		}
		return (vec instanceof DenseVector) ? (DenseVector) vec : ((SparseVector) vec).toDenseVector();
	}

	/**
	 * Get the vector from {@link Vector}, String or Number.
	 * The difference to the {@link #getVector(Object)} is
	 * that it will throw the {@link IllegalArgumentException}
	 * when the obj is a DenseVector.
	 *
	 * @param obj the object casted to vector.
	 * @return The casted SparseVector
	 */
	public static SparseVector getSparseVector(Object obj) {
		Vector vec = getVector(obj);
		if (null == vec) {
			return null;
		}

		if (vec instanceof SparseVector) {
			return (SparseVector) vec;
		} else {
			throw new IllegalArgumentException("CAN NOT get SparseVector!");
		}
	}

}
