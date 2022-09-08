package com.alibaba.alink.common.linalg.tensor;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.linalg.tensor.TensorUtil.DoCalcFunctions;
import org.tensorflow.ndarray.DoubleNdArray;
import org.tensorflow.ndarray.FloatNdArray;
import org.tensorflow.ndarray.NdArray;
import org.tensorflow.ndarray.NdArrays;
import org.tensorflow.ndarray.StdArrays;
import org.tensorflow.ndarray.buffer.DataBuffer;
import org.tensorflow.ndarray.buffer.DataBuffers;
import org.tensorflow.ndarray.buffer.FloatDataBuffer;

import java.util.Arrays;

public final class FloatTensor extends NumericalTensor <Float> {

	public FloatTensor(Shape shape) {
		this(NdArrays.ofFloats(shape.toNdArrayShape()));
	}

	public FloatTensor(float data) {
		this(NdArrays.scalarOf(data));
	}

	public FloatTensor(float[] data) {
		this(StdArrays.ndCopyOf(data));
	}

	public FloatTensor(float[][] data) {
		this(StdArrays.ndCopyOf(data));
	}

	public FloatTensor(float[][][] data) {
		this(StdArrays.ndCopyOf(data));
	}

	FloatTensor(FloatNdArray data) {
		super(data, DataType.FLOAT);
	}

	@Override
	void parseFromValueStrings(String[] valueStrings) {
		float[] arr = new float[valueStrings.length];
		for (int i = 0; i < arr.length; i++) {
			arr[i] = Float.parseFloat(valueStrings[i]);
		}
		data.write(DataBuffers.of(arr));
	}

	@Override
	String[] getValueStrings() {
		int isize = Math.toIntExact(size());
		float[] arr = new float[isize];
		FloatDataBuffer buffer = DataBuffers.of(arr, false, false);
		data.read(buffer);
		String[] valueStrs = new String[isize];
		for (int i = 0; i < isize; i += 1) {
			valueStrs[i] = Float.toString(arr[i]);
		}
		return valueStrs;
	}

	public float getFloat(long... coordinates) {
		return ((FloatNdArray) data).getFloat(coordinates);
	}

	public FloatTensor setFloat(float value, long... coordinates) {
		((FloatNdArray) data).setFloat(value, coordinates);
		return this;
	}

	public FloatTensor scale(float v) {
		FloatNdArray f = ((FloatNdArray) getData());

		f.scalars().forEachIndexed((longs, floatNdArray) -> floatNdArray.setFloat(floatNdArray.getFloat() * v));

		return this;
	}

	@Override
	public FloatTensor reshape(Shape newShape) {
		AkPreconditions.checkArgument(newShape.size() == size(), "Shape not matched.");
		FloatDataBuffer buffer = DataBuffers.ofFloats(size());
		data.read(buffer);
		return new FloatTensor(NdArrays.wrap(newShape.toNdArrayShape(), buffer));
	}

	@Override
	public FloatTensor min(int dim, boolean keepDim) {
		return TensorUtil.doCalc(
			this, dim, keepDim,
			new DoCalcFunctions <Float, float[]>() {
				@Override
				public float[] createArray(int size) {
					return new float[size];
				}

				@Override
				public void initial(float[] array) {
					Arrays.fill(array, Float.MAX_VALUE);
				}

				@Override
				public void calc(float[] array, NdArray <Float> ndArray, long[] coords, int index) {
					array[index] = Math.min(ndArray.getObject(coords), array[index]);
				}

				@Override
				public DataBuffer <Float> write(float[] array) {
					return DataBuffers.of(array);
				}
			}
		);
	}

	@Override
	public FloatTensor max(int dim, boolean keepDim) {
		return TensorUtil.doCalc(
			this, dim, keepDim,
			new DoCalcFunctions <Float, float[]>() {
				@Override
				public float[] createArray(int size) {
					return new float[size];
				}

				@Override
				public void initial(float[] array) {
					Arrays.fill(array, -Float.MAX_VALUE);
				}

				@Override
				public void calc(float[] array, NdArray <Float> ndArray, long[] coords, int index) {
					array[index] = Math.max(ndArray.getObject(coords), array[index]);
				}

				@Override
				public DataBuffer <Float> write(float[] array) {
					return DataBuffers.of(array);
				}
			}
		);
	}

	@Override
	public FloatTensor sum(int dim, boolean keepDim) {
		return TensorUtil.doCalc(
			this, dim, keepDim,
			new DoCalcFunctions <Float, float[]>() {
				@Override
				public float[] createArray(int size) {
					return new float[size];
				}

				@Override
				public void initial(float[] array) {
					Arrays.fill(array, 0.0f);
				}

				@Override
				public void calc(float[] array, NdArray <Float> ndArray, long[] coords, int index) {
					array[index] = ndArray.getObject(coords) + array[index];
				}

				@Override
				public DataBuffer <Float> write(float[] array) {
					return DataBuffers.of(array);
				}
			}
		);
	}

	@Override
	public FloatTensor mean(int dim, boolean keepDim) {
		return TensorUtil.doCalc(
			this, dim, keepDim,
			new DoCalcFunctions <Float, float[]>() {
				@Override
				public float[] createArray(int size) {
					return new float[size];
				}

				@Override
				public void initial(float[] array) {
					Arrays.fill(array, 0.0f);
				}

				@Override
				public void calc(float[] array, NdArray <Float> ndArray, long[] coords, int index) {
					array[index] = ndArray.getObject(coords) + array[index];
				}

				@Override
				public DataBuffer <Float> write(float[] array) {
					return DataBuffers.of(array);
				}

				@Override
				public void post(float[] array, int count) {
					float inverse = 1.0f / count;

					for (int i = 0; i < array.length; ++i) {
						array[i] *= inverse;
					}
				}
			}
		);
	}

	public static FloatTensor of(Tensor <?> other) {
		if (other instanceof DoubleTensor) {
			DoubleTensor doubleTensor = (DoubleTensor) other;
			final FloatTensor floatTensor = new FloatTensor(new Shape(doubleTensor.shape()));

			((DoubleNdArray) doubleTensor.getData())
				.scalars()
				.forEachIndexed((longs, doubleNdArray) -> {
					floatTensor.setFloat((float) doubleNdArray.getDouble(), longs);
				});

			return floatTensor;
		} else if (other instanceof FloatTensor) {
			return (FloatTensor) other;
		} else {
			throw new AkUnsupportedOperationException(String.format(
				"Failed to cast to float tensor. tensor type: %s", other.getType()
			));
		}
	}
}
