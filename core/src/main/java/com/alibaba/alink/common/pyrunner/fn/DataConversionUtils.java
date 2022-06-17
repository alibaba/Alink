package com.alibaba.alink.common.pyrunner.fn;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.pyrunner.fn.conversion.DenseVectorWrapper;
import com.alibaba.alink.common.pyrunner.fn.conversion.MTableWrapper;
import com.alibaba.alink.common.pyrunner.fn.conversion.SparseVectorWrapper;
import com.alibaba.alink.common.pyrunner.fn.conversion.TensorWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

public class DataConversionUtils {

	private static final Logger LOG = LoggerFactory.getLogger(DataConversionUtils.class);

	/**
	 * Convert a Java object to another one from which Py4j can easily construct a pure-Python equivalent for UDF/UDTF
	 * processing.
	 *
	 * @param obj
	 * @return
	 */
	public static Object javaToPy(Object obj) {
		if (obj instanceof Tensor) {
			return TensorWrapper.fromJava((Tensor <?>) obj);
		} else if (obj instanceof DenseVector) {
			return DenseVectorWrapper.fromJava((DenseVector) obj);
		} else if (obj instanceof SparseVector) {
			return SparseVectorWrapper.fromJava((SparseVector) obj);
		} else if (obj instanceof MTable) {
			return MTableWrapper.fromJava((MTable) obj);
		} else {
			return obj;
		}
	}

	/**
	 * Convert a Java object from Python side to Java.
	 * <p>
	 * Though Py4j automatically convert Python values to Java values, but usually not with expected types. For example,
	 * Python function can return an int value to represent a timestamp value, which has to be manually converted to
	 * java.sql.Timestamp.
	 * <p>
	 * For Alink-defined types, i.e. Tensors, Vectors and MTables, conversion is also required.
	 *
	 * @param obj
	 * @param t
	 * @return
	 */
	public static Object pyToJava(Object obj, TypeInformation <?> t) {
		if (obj == null) {
			return null;
		}

		// If `obj` is an instance of `JavaObjectWrapper`, directly return its wrapped object.
		if (obj instanceof JavaObjectWrapper) {
			return ((JavaObjectWrapper <?>) obj).getJavaObject();
		}

		// For `obj` of other primitive types, try convert it based on its type information.
		if (t.equals(AlinkTypes.BOOLEAN)) {
			if (obj instanceof Boolean) {
				return obj;
			}
			return Boolean.valueOf(obj.toString());
		} else if (t.equals(AlinkTypes.BYTE)) {
			if (obj instanceof Number) {
				return ((Number) obj).byteValue();
			}
			return Byte.valueOf(obj.toString());
		} else if (t.equals(AlinkTypes.SHORT)) {
			if (obj instanceof Number) {
				return ((Number) obj).shortValue();
			}
			return Short.valueOf(obj.toString());
		} else if (t.equals(AlinkTypes.INT)) {
			if (obj instanceof Number) {
				return ((Number) obj).intValue();
			}
			return Integer.valueOf(obj.toString());
		} else if (t.equals(AlinkTypes.LONG)) {
			if (obj instanceof Number) {
				return ((Number) obj).longValue();
			}
			return Long.valueOf(obj.toString());
		} else if (t.equals(AlinkTypes.FLOAT)) {
			if (obj instanceof Number) {
				return ((Number) obj).floatValue();
			}
			return Float.valueOf(obj.toString());
		} else if (t.equals(AlinkTypes.DOUBLE)) {
			if (obj instanceof Number) {
				return ((Number) obj).doubleValue();
			}
			return Double.valueOf(obj.toString());
		} else if (t.equals(AlinkTypes.BIG_INT)) {
			if (obj instanceof BigInteger) {
				return obj;
			} else {
				return new BigInteger(obj.toString());
			}
		} else if (t.equals(AlinkTypes.BIG_DEC)) {
			if (obj instanceof BigDecimal) {
				return obj;
			}
			return new BigDecimal(obj.toString());
		} else if (t.equals(AlinkTypes.STRING)) {
			if (obj instanceof String) {
				return obj;
			}
			return obj.toString();
		} else if (t.equals(AlinkTypes.SQL_DATE)) {
			if (obj instanceof Number) {
				// In scalar function returning SQL_DATE, Python function returns #days since epoch, and Flink
				// automatically convert int value to java.sql.Date through SqlFunctions#internalToDate.
				// So, in table function, we keep the same conversion.
				return Date.valueOf(LocalDate.ofEpochDay(((Number) obj).longValue()));
			}
			if (obj instanceof Date) {
				return obj;
			}
			return Date.valueOf(obj.toString());
		} else if (t.equals(AlinkTypes.SQL_TIME)) {
			if (obj instanceof Number) {
				// In scalar function returning SQL_TIME, Python function returns milliseconds of a day, and Flink
				// automatically convert int value to java.sql.Date through SqlFunctions#internalToTime.
				// So, in table function, we keep the same conversion.
				return Time.valueOf(LocalTime.ofNanoOfDay(((Number) obj).longValue() * 1000000));
			}
			if (obj instanceof Time) {
				return obj;
			}
			return Time.valueOf(obj.toString());
		} else if (t.equals(AlinkTypes.SQL_TIMESTAMP)) {
			if (obj instanceof Number) {
				// In scalar function returning SQL_TIMESTAMP, Python function returns milliseconds since epoch, and
				// Flink automatically convert int value to java.sql.Date through SqlFunctions#internalToTimestamp.
				// So, in table function, we keep the same conversion.
				// NOTE: directly using new Timestamp(obj) gives a wrong value.
				Instant instant = Instant.ofEpochMilli(((Number) obj).longValue());
				return Timestamp.valueOf(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
			}
			if (obj instanceof Timestamp) {
				return obj;
			}
			return Timestamp.valueOf(obj.toString());
		} else {
			LOG.info("Unsupported type: " + t);
			return obj;
		}
	}
}
