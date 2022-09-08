package com.alibaba.alink.common.linalg;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

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

	final static int zeroAscii = '0';
	final static int nineAscii = '9';
	final static char spaceChar = ' ';
	final static char commaChar = ',';

	/**
	 * Parse either a {@link SparseVector} or a {@link DenseVector} from a formatted string.
	 *
	 * <p>The format of a dense vector is space separated values such as "1 2 3 4".
	 * The format of a sparse vector is space separated index-value pairs, such as "0:1 2:3 3:4". If the sparse vector
	 * has determined vector size, the size is prepended to the head. For example, the string "$4$0:1 2:3 3:4"
	 * represents a sparse vector with size 4.
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
				|| c == commaChar) {
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
		int lastInCharBuffPos = 0;

		inDataBuffPos = 0;
		isInBuff = false;

		for (int i = 0; i < len; ++i) {
			char c = str.charAt(i);

			if (c == ELEMENT_DELIMITER
				// to be compatible with previous delimiter
				|| c == commaChar) {
				if (isInBuff) {
					data[inDataBuffPos++] = Double.parseDouble(
						StringUtils.substring(str, lastInCharBuffPos, i).trim()
					);

					lastInCharBuffPos = i + 1;
				}

				isInBuff = false;
			} else {
				isInBuff = true;
			}
		}

		if (isInBuff) {
			data[inDataBuffPos] = Double.parseDouble(
				StringUtils.substring(str, lastInCharBuffPos).trim()
			);
		}

		return new DenseVector(data);
	}

	private static final Logger LOG = LoggerFactory.getLogger(VectorUtil.class);
	private static int parseExceptionCounter = 0;
	private static final int parseExceptionPrintNum = 10;
	private static final ThreadLocal <double[]> threadSafeDataBuffer = ThreadLocal.withInitial(() -> new double[4096]);
	private static final ThreadLocal <int[]> threadSafeIndexBuffer = ThreadLocal.withInitial(() -> new int[4096]);

	/**
	 * Parse the sparse vector from a formatted string.
	 *
	 * <p>The format of a sparse vector is space separated index-value pairs, such as "0:1 2:3 3:4".
	 * If the sparse vector has determined vector size, the size is prepended to the head. For example, the string
	 * "$4$0:1 2:3 3:4" represents a sparse vector with size 4.
	 *
	 * @throws IllegalArgumentException If the string is of invalid format.
	 */
	public static SparseVector parseSparse(String str) {
		return parseSparse(str, true);
	}

	public static SparseVector parseSparse(String str, boolean isPrecise) {
		if (org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(str)) {
			return new SparseVector();
		}
		double[] dataBuffer = threadSafeDataBuffer.get();
		int[] indexBuffer = threadSafeIndexBuffer.get();
		char sparseElementDelimiter = ELEMENT_DELIMITER;
		if (str.indexOf(commaChar) != -1) {
			sparseElementDelimiter = commaChar;
		}

		int pos;
		int indexStartPos;
		int indexEndPos;
		int valueStartPos;
		int valueEndPos;
		int idx = 0;
		char c;
		int n = str.length();
		int dim = -1;

		if ((pos = str.indexOf(HEADER_DELIMITER)) != -1) {
			int anotherHeaderPos = str.indexOf(HEADER_DELIMITER, ++pos);
			if (anotherHeaderPos == -1) {
				String errMsg = String.format(
					"Statement of dimension should end with '$'. Fail to getVector sparse vector from string. "
						+ "%s", str);
				if (isPrecise) {
					throw new AkIllegalDataException(errMsg);
				} else {
					LOG.error(errMsg);
				}
				parseExceptionCounter++;
				if (parseExceptionCounter > parseExceptionPrintNum) {
					throw new AkIllegalDataException(errMsg);
				}
			}
			try {
				dim = parseInt(str, pos, anotherHeaderPos);
			} catch (Exception e) {
				String errMsg = "Sparse vector size parse err. @ " + str;
				if (isPrecise) {
					throw new AkIllegalDataException(errMsg);
				} else {
					LOG.error(errMsg);
				}
			}
			pos = anotherHeaderPos + 1;
		}
		pos = pos == -1 ? 0 : pos;

		while (pos < n) {
			indexStartPos = pos;
			// if char at pos is ':', ',' or last char of the string, the jump out loop.
			while (pos < n && (c = str.charAt(pos)) != INDEX_VALUE_DELIMITER && c != commaChar) {
				pos++;
			}
			if (pos >= n || str.charAt(pos) == commaChar) {
				pos++;
				continue;
			}
			indexEndPos = pos++;
			pos = forwardTrim(str, pos, n);
			valueStartPos = pos;
			// find the first delimiter after the value.
			while (pos < n && str.charAt(pos) != sparseElementDelimiter) {
				pos++;
			}
			valueEndPos = backwardTrim(str, pos, valueStartPos);
			pos = forwardTrim(str, pos, n);
			if (pos < n && str.charAt(pos) == INDEX_VALUE_DELIMITER) {
				if (isPrecise) {
					throw new AkIllegalDataException("Sparse vector parse err. " + " @ " + str);
				} else {
					if (parseExceptionCounter < parseExceptionPrintNum) {
						LOG.error("Sparse vector parse err. " + " @ " + str);
					}
					parseExceptionCounter++;
				}
				pos++;
				pos = forwardTrim(str, pos, n);
				while (pos < n && str.charAt(pos) != sparseElementDelimiter && str.charAt(pos)
					!= INDEX_VALUE_DELIMITER) {
					pos++;
				}
				continue;
			}
			try {
				indexBuffer[idx] = parseInt(str, indexStartPos, indexEndPos);
				dataBuffer[idx] = parseDouble(str, valueStartPos, valueEndPos);
				idx++;
			} catch (Exception e) {
				if (isPrecise) {
					throw new AkIllegalDataException(e.getMessage() + " @ " + str);
				} else {
					if (parseExceptionCounter < parseExceptionPrintNum) {
						LOG.error(e.getMessage() + " @ " + str);
					}
					parseExceptionCounter++;
				}
			}
			if (idx == indexBuffer.length) {
				int[] tmpIndexBuffer = new int[indexBuffer.length * 2];
				double[] tmpDataBuffer = new double[dataBuffer.length * 2];
				System.arraycopy(dataBuffer, 0, tmpDataBuffer, 0, dataBuffer.length);
				System.arraycopy(indexBuffer, 0, tmpIndexBuffer, 0, indexBuffer.length);
				threadSafeIndexBuffer.set(tmpIndexBuffer);
				threadSafeDataBuffer.set(tmpDataBuffer);
				indexBuffer = threadSafeIndexBuffer.get();
				dataBuffer = threadSafeDataBuffer.get();
			}
		}

		int[] indices = new int[idx];
		double[] data = new double[idx];
		System.arraycopy(indexBuffer, 0, indices, 0, idx);
		System.arraycopy(dataBuffer, 0, data, 0, idx);
		return new SparseVector(dim, indices, data);
	}

	private static int forwardTrim(String str, int pos, int endPos) {
		while (pos < endPos && str.charAt(pos) <= spaceChar) {
			pos++;
		}
		return pos;
	}

	private static int backwardTrim(String str, int pos, int startPos) {
		while (pos > startPos && str.charAt(pos - 1) <= spaceChar) {
			pos--;
		}
		return pos;
	}

	private static int indexOfDot(String str, int startPos, int endPos) {
		for (int i = startPos; i < endPos; ++i) {
			if (str.charAt(i) == '.') {
				return i;
			}
		}
		return -1;
	}

	private static int IndexOfE(String str, int startPos, int endPos) {
		for (int i = endPos - 1; i > startPos; --i) {
			if (str.charAt(i) == 'E' || str.charAt(i) == 'e') {
				return i;
			}
			if (endPos - i > 5) {
				return -1;
			}
		}
		return -1;
	}

	public static double parseDouble(String str, int startPos, int endPos) {
		if (startPos == endPos) {
			throw new AkIllegalDataException(String.format("Failed parseDouble. position %d ", startPos));
		}
		int expPos = IndexOfE(str, startPos, endPos);
		int dotPos = indexOfDot(str, startPos, endPos);
		double intPart = 0.0;
		double lowPart = 0.0;
		int expPart = 0;
		double sign = 1.0;

		if (str.charAt(0) == '-') {
			sign = -1.0;
			startPos = 1;
		} else if (str.charAt(0) == '+') {
			startPos = 1;
		}
		int endIPos = (dotPos != -1) ? dotPos : (expPos != -1 ? expPos : endPos);
		for (int i = startPos; i < endIPos; ++i) {
			char current = str.charAt(i);
			if (current > nineAscii || current < zeroAscii) {
				String subStr = str.substring(startPos, endPos);
				return Double.parseDouble(subStr);
			}
			intPart = (intPart + (current - zeroAscii)) * 10.0;
		}
		if (dotPos != -1) {
			int endLPos = expPos != -1 ? expPos : endPos;
			if (endLPos - dotPos < 20) {
				long ratio = 1L;
				long result = 0L;
				for (int i = dotPos + 1; i < endLPos; ++i) {
					char current = str.charAt(i);
					if (current > nineAscii || current < zeroAscii) {
						String subStr = str.substring(startPos, endPos);
						return Double.parseDouble(subStr);
					}
					result *= 10L;
					result += current - zeroAscii;
					ratio *= 10L;
				}
				lowPart = (double) result / (double) ratio;
			} else {
				double ratio = 0.1;
				for (int i = dotPos + 1; i < endLPos; ++i) {
					char current = str.charAt(i);
					if (current > nineAscii || current < zeroAscii) {
						String subStr = str.substring(startPos, endPos);
						return Double.parseDouble(subStr);
					}
					lowPart += (current - zeroAscii) * ratio;
					ratio *= 0.1;
				}
			}
		}
		if (expPos == -1) {
			return sign * (intPart * 0.1 + lowPart);
		} else {
			double expBase = 10;
			if (str.charAt(expPos + 1) == '-') {
				expPos += 1;
				expBase = 0.1;
			}
			for (int i = expPos + 1; i < endPos; ++i) {
				char current = str.charAt(i);
				if (current > nineAscii || current < zeroAscii) {
					return Double.parseDouble(str.substring(startPos, endPos));
				}
				expPart *= 10;
				expPart += (current - zeroAscii);
			}
			return sign * (intPart * 0.1 + lowPart) * Math.pow(expBase, expPart);
		}
	}

	private static int parseInt(String str, int startPos, int endPos) {
		int ret = 0;
		char c;
		startPos = forwardTrim(str, startPos, endPos);
		endPos = backwardTrim(str, endPos, startPos);

		if (startPos == endPos) {
			throw new AkIllegalDataException(String.format("Failed parseInt. position %d ", startPos));
		}
		while ((c = str.charAt(startPos)) <= nineAscii && c >= zeroAscii) {
			ret *= 10;
			ret += c - zeroAscii;
			startPos++;
		}
		if (startPos != endPos) {
			return robustParseInt(str, startPos, endPos);
		}
		return ret;
	}

	private static int robustParseInt(String str, int startPos, int endPos) {
		String[] indexArr = str.substring(startPos, endPos).split(" ");
		return Integer.parseInt(indexArr[indexArr.length - 1]);
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
	 * Serialize the vector to a string.
	 */
	public static String serialize(Object vector) {
		return toString((Vector) vector);
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
			throw new AkIllegalDataException("Can not get the vector from " + obj);
		}
	}

	/**
	 * Get the vector from {@link Vector}, String or Number. The difference to the {@link #getVector(Object)} is
	 * that it
	 * will translate the SparseVector to DenseVector when the obj is a SparseVector.
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
	 * Get the vector from {@link Vector}, String or Number. The difference to the {@link #getVector(Object)} is
	 * that it
	 * will throw the {@link IllegalArgumentException} when the obj is a DenseVector.
	 *
	 * @param obj the object casted to vector.
	 * @return The casted SparseVector
	 */
	public static SparseVector getSparseVector(Object obj) {
		Vector vec = getVector(obj);
		if (null == vec) {
			return null;
		}
		return (vec instanceof SparseVector) ? (SparseVector) vec : ((DenseVector) vec).toSparseVector();
	}

	/**
	 * decode the given bytes to a Vector: The format of the byte[] is: "size key1 value1 key2 value2..."
	 *
	 * @param bytes bytes array of SparseVector.
	 * @return SparseVector
	 */
	public static SparseVector decodeSparseVector(byte[] bytes) {
		AkPreconditions.checkArgument((bytes.length - 4 - 1) % 12 == 0);
		int size = bytes.length / 12;
		ByteBuffer wrapper = ByteBuffer.wrap(bytes);
		wrapper.get(); // pass the first byte
		int[] indices = new int[size];
		int vectorSize = wrapper.getInt();
		double[] values = new double[size];
		for (int i = 0; i < size; i++) {
			indices[i] = wrapper.getInt();
			values[i] = wrapper.getDouble();
		}
		return new SparseVector(vectorSize, indices, values);
	}

	/**
	 * decode the given bytes to a Vector. The format of the byte[] is: "value1 value2 value3..."
	 *
	 * @param bytes bytes array of DenseVector.
	 * @return DenseVector
	 */
	private static DenseVector decodeDenseVector(byte[] bytes) {
		AkPreconditions.checkArgument((bytes.length - 1) % 8 == 0);
		int size = bytes.length / 8;
		ByteBuffer wrapper = ByteBuffer.wrap(bytes);
		wrapper.get(); // pass the first byte
		double[] value = new double[size];
		for (int i = 0; i < size; i++) {
			value[i] = wrapper.getDouble();
		}
		return new DenseVector(value);
	}

	public static Vector fromBytes(byte[] bytes) {
		switch (bytes[0]) {
			case VectorSerialType.DENSE_VECTOR:
				return decodeDenseVector(bytes);
			case VectorSerialType.SPARSE_VECTOR:
				return decodeSparseVector(bytes);
		}
		throw new AkUnsupportedOperationException("Unsupported Vector Type");
	}

	static class VectorSerialType {
		public static final byte DENSE_VECTOR = (byte) 0;
		public static final byte SPARSE_VECTOR = (byte) 1;
	}

}
