package com.alibaba.alink.common.dl.plugin;

import com.alibaba.alink.common.linalg.tensor.BoolTensor;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.LongTensor;
import com.alibaba.alink.common.linalg.tensor.StringTensor;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import org.pytorch.IValue;
import org.pytorch.Module;
import org.pytorch.Tensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.alink.common.dl.utils.PyTorchTensorConversionUtils.fromPyTorchTensor;
import static com.alibaba.alink.common.dl.utils.PyTorchTensorConversionUtils.toPTTensor;

/**
 * This class provides a base wrapper for PyTorch predictor. This class is not thread-safe, as {@link Module} not.
 */
public class TorchJavaPredictor implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(TorchJavaPredictor.class);

	private final String modelPath;
	private final Class <?>[] outputTypeClasses;
	private Module module;
	private long counter = 0;

	public TorchJavaPredictor(String modelPath, Class <?>[] outputTypeClasses) {
		this.modelPath = modelPath;
		this.outputTypeClasses = outputTypeClasses;
	}

	public void open() {
		module = Module.load(this.modelPath);
	}

	public List <?> predict(List <?> inputs) {
		int numInputs = inputs.size();
		IValue[] inputIValues = new IValue[numInputs];
		for (int i = 0; i < numInputs; i += 1) {
			inputIValues[i] = javaToIValue(inputs.get(i));
		}
		IValue output = module.forward(inputIValues);
		List <IValue> outputIValues = unwrapIValue(output);
		List <Object> outputs = new ArrayList <>();
		for (int i = 0; i < outputTypeClasses.length; i += 1) {
			IValue outputIValue = outputIValues.get(i);
			Object obj = iValueToJavaWithType(outputIValue, outputTypeClasses[i]);
			outputs.add(obj);
		}
		counter += 1;
		return outputs;
	}

	public void close() {
		module.destroy();
		LOG.info("Total records processed: " + counter);
	}

	/**
	 * If v is list or tuple, unwrap it to a list of IValue's
	 */
	public static List <IValue> unwrapIValue(IValue v) {
		List <IValue> outputs = new ArrayList <>();
		if (v.isTuple()) {
			IValue[] iValues = v.toTuple();
			outputs.addAll(Arrays.asList(iValues));
		} else if (v.isList()) {
			outputs.addAll(Arrays.asList(v.toList()));
		} else if (v.isTensorList()) {
			Tensor[] tensors = v.toTensorList();
			for (Tensor tensor : tensors) {
				outputs.add(IValue.from(tensor));
			}
		} else if (v.isBoolList()) {
			boolean[] booleans = v.toBoolList();
			for (boolean aBoolean : booleans) {
				outputs.add(IValue.from(aBoolean));
			}
		} else if (v.isDoubleList()) {
			double[] doubles = v.toDoubleList();
			for (double aDouble : doubles) {
				outputs.add(IValue.from(aDouble));
			}
		} else if (v.isLongList()) {
			long[] longs = v.toLongList();
			for (long aLong : longs) {
				outputs.add(IValue.from(aLong));
			}
		} else if (v.isDictLongKey() || v.isDictStringKey()) {
			throw new RuntimeException("Dict is not supported in output");
		} else {
			outputs.add(v);
		}
		return outputs;
	}

	/**
	 * Convert a Java object to IValue.
	 */
	public static IValue javaToIValue(Object v) {
		if (v instanceof com.alibaba.alink.common.linalg.tensor.Tensor) {
			com.alibaba.alink.common.linalg.tensor.Tensor <?> t = TensorUtil.getTensor(v);
			return IValue.from(toPTTensor(t));
		} else if (v instanceof Boolean) {
			return IValue.from((Boolean) v);
		} else if (v instanceof Integer || v instanceof Long || v instanceof BigInteger) {
			return IValue.from(((Number) v).longValue());
		} else if (v instanceof Double || v instanceof Float || v instanceof BigDecimal) {
			return IValue.from(((Number) v).doubleValue());
		} else if (v instanceof String) {
			return IValue.from((String) v);
		}
		throw new IllegalArgumentException(
			String.format(
				"Failed to convert value %s of type %s to a TorchScript IValue. "
					+ "Can only use Tensor types or compatible types of Boolean, Long, Double, and String.",
				v, v.getClass().getSimpleName()));
	}

	/**
	 * Convert an IValue to a Java object, where iValue can only be simple type.
	 */
	static Object iValueToJava(IValue iValue) {
		if (iValue.isNull()) {
			return null;
		} else if (iValue.isTensor()) {
			return fromPyTorchTensor(iValue.toTensor());
		} else if (iValue.isBool()) {
			return iValue.toBool();
		} else if (iValue.isLong()) {
			return iValue.toLong();
		} else if (iValue.isDouble()) {
			return iValue.toDouble();
		} else if (iValue.isString()) {
			return iValue.toStr();
		} else {
			throw new RuntimeException("IValue is not a simple type: " + iValue);
		}
	}

	/**
	 * Convert IValue to a Java object of given type class, where iValue can only be simple type.
	 **/
	public static Object iValueToJavaWithType(IValue iValue, Class <?> clazz) {
		Object v = iValueToJava(iValue);
		if (null == v) {
			return null;
		}
		// to Alink Tensor
		if (com.alibaba.alink.common.linalg.tensor.Tensor.class.isAssignableFrom(clazz)) {
			if (v instanceof com.alibaba.alink.common.linalg.tensor.Tensor) {
				return v;
			} else if (v instanceof Boolean) {
				return new BoolTensor(new boolean[] {(Boolean) v});
			} else if (v instanceof Long) {
				return new LongTensor(new long[] {(Long) v});
			} else if (v instanceof Double) {
				return new DoubleTensor(new double[] {(Double) v});
			} else if (v instanceof String) {
				return new StringTensor(new String[] {(String) v});
			} else {
				throw new RuntimeException(String.format("Cannot convert value %s to type %s.",
					v, clazz.getSimpleName()));
			}
		}
		// to primitive types or String
		return clazz.equals(String.class) ? v.toString() : JsonConverter.fromJson(v.toString(), clazz);
	}
}
