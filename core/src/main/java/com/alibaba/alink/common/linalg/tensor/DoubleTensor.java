package com.alibaba.alink.common.linalg.tensor;

import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.tensor.TensorUtil.DoCalcFunctions;
import org.tensorflow.ndarray.DoubleNdArray;
import org.tensorflow.ndarray.FloatNdArray;
import org.tensorflow.ndarray.IntNdArray;
import org.tensorflow.ndarray.LongNdArray;
import org.tensorflow.ndarray.NdArray;
import org.tensorflow.ndarray.NdArrays;
import org.tensorflow.ndarray.StdArrays;
import org.tensorflow.ndarray.buffer.DataBuffer;
import org.tensorflow.ndarray.buffer.DataBuffers;
import org.tensorflow.ndarray.buffer.DoubleDataBuffer;

import java.util.Arrays;

public final class DoubleTensor extends NumericalTensor <Double> {

	public DoubleTensor(Shape shape) {
		this(NdArrays.ofDoubles(shape.toNdArrayShape()));
	}

	public DoubleTensor(double[] data) {
		this(StdArrays.ndCopyOf(data));
	}

	public DoubleTensor(double[][] data) {
		this(StdArrays.ndCopyOf(data));
	}

	public DoubleTensor(double[][][] data) {
		this(StdArrays.ndCopyOf(data));
	}

	DoubleTensor(DoubleNdArray data) {
		super(data, DataType.DOUBLE);
	}

	@Override
	void parseFromValueStrings(String[] valueStrings) {
		double[] arr = new double[valueStrings.length];
		for (int i = 0; i < arr.length; i++) {
			arr[i] = Double.parseDouble(valueStrings[i]);
		}
		data.write(DataBuffers.of(arr));
	}

	@Override
	String[] getValueStrings() {
		int size = Math.toIntExact(size());
		double[] arr = new double[size];
		DoubleDataBuffer buffer = DataBuffers.of(arr, false, false);
		data.read(buffer);
		String[] valueStrings = new String[size];
		for (int i = 0; i < size; i += 1) {
			valueStrings[i] = Double.toString(arr[i]);
		}
		return valueStrings;
	}

	@Override
	public DoubleTensor reshape(Shape newShape) {
		Preconditions.checkArgument(newShape.size() == size(), "Shape not matched.");
		DoubleDataBuffer buffer = DataBuffers.ofDoubles(size());
		data.read(buffer);
		return new DoubleTensor(NdArrays.wrap(newShape.toNdArrayShape(), buffer));
	}

	@Override
	public DoubleTensor min(int dim, boolean keepDim) {
		return TensorUtil.doCalc(
			this, dim, keepDim,
			new DoCalcFunctions <Double, double[]>() {
				@Override
				public double[] createArray(int size) {
					return new double[size];
				}

				@Override
				public void initial(double[] array) {
					Arrays.fill(array, Double.MAX_VALUE);
				}

				@Override
				public void calc(double[] array, NdArray <Double> ndArray, long[] coords, int index) {
					array[index] = Math.min(ndArray.getObject(coords), array[index]);
				}

				@Override
				public DataBuffer <Double> write(double[] array) {
					return DataBuffers.of(array);
				}
			}
		);
	}

	@Override
	public DoubleTensor max(int dim, boolean keepDim) {
		return TensorUtil.doCalc(
			this, dim, keepDim,
			new DoCalcFunctions <Double, double[]>() {
				@Override
				public double[] createArray(int size) {
					return new double[size];
				}

				@Override
				public void initial(double[] array) {
					Arrays.fill(array, -Double.MAX_VALUE);
				}

				@Override
				public void calc(double[] array, NdArray <Double> ndArray, long[] coords, int index) {
					array[index] = Math.max(ndArray.getObject(coords), array[index]);
				}

				@Override
				public DataBuffer <Double> write(double[] array) {
					return DataBuffers.of(array);
				}
			}
		);
	}

	@Override
	public DoubleTensor sum(int dim, boolean keepDim) {
		return TensorUtil.doCalc(
			this, dim, keepDim,
			new DoCalcFunctions <Double, double[]>() {
				@Override
				public double[] createArray(int size) {
					return new double[size];
				}

				@Override
				public void initial(double[] array) {
					Arrays.fill(array, 0.0);
				}

				@Override
				public void calc(double[] array, NdArray <Double> ndArray, long[] coords, int index) {
					array[index] = ndArray.getObject(coords) + array[index];
				}

				@Override
				public DataBuffer <Double> write(double[] array) {
					return DataBuffers.of(array);
				}
			}
		);
	}

	@Override
	public DoubleTensor mean(int dim, boolean keepDim) {
		return TensorUtil.doCalc(
			this, dim, keepDim,
			new DoCalcFunctions <Double, double[]>() {
				@Override
				public double[] createArray(int size) {
					return new double[size];
				}

				@Override
				public void initial(double[] array) {
					Arrays.fill(array, 0.0);
				}

				@Override
				public void calc(double[] array, NdArray <Double> ndArray, long[] coords, int index) {
					array[index] = ndArray.getObject(coords) + array[index];
				}

				@Override
				public DataBuffer <Double> write(double[] array) {
					return DataBuffers.of(array);
				}

				@Override
				public void post(double[] array, int count) {
					double inverse = 1.0 / count;

					for (int i = 0; i < array.length; ++i) {
						array[i] *= inverse;
					}
				}
			}
		);
	}

	public double getDouble(long... coordinates) {
		return ((DoubleNdArray) data).getDouble(coordinates);
	}

	public DoubleTensor setDouble(double value, long... coordinates) {
		((DoubleNdArray) data).setDouble(value, coordinates);
		return this;
	}

	public DenseVector toVector() {
		double[] arr = new double[Math.toIntExact(size())];
		DoubleDataBuffer buffer = DataBuffers.of(arr, false, false);
		data.read(buffer);
		return new DenseVector(arr);
	}

	public static DoubleTensor of(Tensor <?> other) {
		if (!(other instanceof NumericalTensor)) {
			throw new UnsupportedOperationException(
				String.format(
					"Only numerical tensor can ben cast to double tensor. tensor type: %s", other.getType()
				)
			);
		}

		DataType otherDType = other.getType();

		switch (otherDType) {
			case DOUBLE:
				return (DoubleTensor) other;
			case FLOAT:
				FloatTensor fTensor = (FloatTensor) other;
				final DoubleTensor f2dTensor = new DoubleTensor(new Shape(fTensor.shape()));

				((FloatNdArray) fTensor.getData())
					.scalars()
					.forEachIndexed(
						(longs, doubleNdArray) -> f2dTensor.setDouble(doubleNdArray.getFloat(), longs)
					);

				return f2dTensor;
			case INT:
				IntTensor iTensor = (IntTensor) other;
				final DoubleTensor i2dTensor = new DoubleTensor(new Shape(iTensor.shape()));

				((IntNdArray) iTensor.getData())
					.scalars()
					.forEachIndexed(
						(longs, intNdArray) -> i2dTensor.setDouble(intNdArray.getInt(), longs)
					);

				return i2dTensor;
			case LONG:
				LongTensor lTensor = (LongTensor) other;
				final DoubleTensor l2dTensor = new DoubleTensor(new Shape(lTensor.shape()));

				((LongNdArray) lTensor.getData())
					.scalars()
					.forEachIndexed(
						(longs, longNdArray) -> l2dTensor.setDouble(longNdArray.getLong(), longs)
					);

				return l2dTensor;
			default:
				throw new UnsupportedOperationException();
		}
	}
}
