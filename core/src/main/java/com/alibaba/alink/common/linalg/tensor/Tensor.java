package com.alibaba.alink.common.linalg.tensor;

import com.alibaba.alink.common.viz.DataTypeDisplayInterface;
import com.alibaba.alink.common.linalg.tensor.TensorUtil.CoordInc;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.tensorflow.ndarray.NdArray;

import java.io.Serializable;
import java.lang.reflect.Array;

public abstract class Tensor<DT> implements Serializable, DataTypeDisplayInterface {
	protected DataType type;
	protected NdArray <DT> data;

	public long[] shape() {
		return data.shape().asArray();
	}

	public long size() {
		return data.shape().size();
	}

	public DT getObject(long... coordinates) {
		return data.getObject(coordinates);
	}

	public Tensor <DT> setObject(DT value, long... coordinates) {
		data.setObject(value, coordinates);
		return this;
	}

	@Override
	public String toString() {
		return toDisplaySummary() +"\n" + toShortDisplayData();
	}

	public abstract Tensor <DT> reshape(Shape newShape);

	/**
	 * Flattens the tensor by reshaping it into a rank-1 tensor.
	 *
	 * @param startDim the start dimension. inclusive.
	 * @param endDim   the end dimension. inclusive.
	 * @return the flattened tensor.
	 */
	public Tensor <DT> flatten(int startDim, int endDim) {

		long[] shape = shape();
		int size = shape.length;

		int wrappedStartDim = (int) TensorUtil.wrapDim(startDim, size);
		int wrappedEndDim = (int) TensorUtil.wrapDim(endDim, size);

		long[] newShape = new long[size - (wrappedEndDim - wrappedStartDim)];

		// part 1.
		if (wrappedStartDim > 0) {
			System.arraycopy(shape, 0, newShape, 0, wrappedStartDim);
		}

		// flattened
		long flattened = 1L;
		for (int i = wrappedStartDim; i <= wrappedEndDim; ++i) {
			flattened *= shape[i];
		}
		newShape[wrappedStartDim] = flattened;

		// skip the flattened shape.
		wrappedStartDim += 1;
		wrappedEndDim += 1;

		// part 3.
		if (wrappedEndDim < size) {
			System.arraycopy(
				shape, wrappedEndDim, newShape, wrappedStartDim, size - wrappedEndDim
			);
		}

		return reshape(new Shape(newShape));
	}

	public Tensor <DT> flatten() {
		return flatten(0, -1);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof Tensor)) {
			return false;
		}
		Tensor <?> t = (Tensor <?>) obj;
		if (!type.equals(t.type)) {
			return false;
		}
		return data.equals(t.data);
	}

	/**
	 * Stacks a list of rank-R tensors into one rank-(R+1) tensor.
	 *
	 * @param tensors       a list of tensors with same shape and type.
	 * @param dim           The dimension to stack along.
	 * @param stackedTensor the stacked tensor whose data will be filled.
	 * @param <DT>          data type
	 */
	public static <DT, T extends Tensor <DT>>
	T stack(T[] tensors, int dim, T stackedTensor) {

		if (tensors == null || tensors.length == 0) {
			return stackedTensor;
		}

		T first = tensors[0];

		int wrappedDim = (int) TensorUtil.wrapDim(dim, first.shape().length);

		long[] shape = ArrayUtils.add(first.shape(), wrappedDim, tensors.length);

		if (stackedTensor == null) {
			stackedTensor = TensorUtil.of(shape, first.type);
		}

		long[] outterCoords = new long[wrappedDim];

		int outer = 1;
		for (int i = 0; i < wrappedDim; ++i) {
			outer *= shape[i];
		}

		CoordInc coordInc = new CoordInc(shape, wrappedDim, outterCoords);

		for (int i = 0; i < outer; ++i) {

			long[] innerCoords = ArrayUtils.add(outterCoords, 0L);

			for (int j = 0; j < tensors.length; ++j) {
				innerCoords[wrappedDim] = j;

				stackedTensor.getData().set(tensors[j].getData().get(outterCoords), innerCoords);
			}

			coordInc.inc();
		}

		return stackedTensor;
	}

	/**
	 * Unpacks the given dimension of a rank-R tensor into rank-(R-1) tensors.
	 *
	 * @param stackedTensor the stacked tensor.
	 * @param dim           The dimension to unstack along.
	 * @param tensors       a list of tensors with same shape and type whose data will be filled.
	 * @param <DT>          data type
	 */
	public static <DT, T extends Tensor <DT>> T[] unstack(
		T stackedTensor, int dim, T[] tensors) {

		long[] shape = stackedTensor.shape();

		int wrappedDim = (int) TensorUtil.wrapDim(dim, shape.length);

		long[] oShape = ArrayUtils.remove(shape, wrappedDim);

		if (tensors == null) {
			int size = (int) shape[wrappedDim];

			tensors = (T[]) Array.newInstance(stackedTensor.getClass(), size);

			for (int i = 0; i < shape[wrappedDim]; ++i) {
				tensors[i] = TensorUtil.of(oShape, stackedTensor.getType());
			}
		}

		long[] outterCoords = new long[wrappedDim + 1];
		int outer = 1;

		for (int i = 0; i < wrappedDim; ++i) {
			outer *= shape[i];
		}

		CoordInc coordInc = new CoordInc(shape, wrappedDim, outterCoords);

		for (int i = 0; i < outer; ++i) {
			long[] innerCoords = ArrayUtils.subarray(outterCoords, 0, wrappedDim);

			for (int j = 0; j < tensors.length; ++j) {
				outterCoords[wrappedDim] = j;
				tensors[j].getData().set(stackedTensor.getData().get(outterCoords), innerCoords);
			}

			coordInc.inc();
		}

		return tensors;
	}

	public static <DT, T extends Tensor <DT>>
	T cat(T[] tensors, int dim, T cattedTensor) {

		if (tensors == null || tensors.length == 0) {
			return cattedTensor;
		}

		T first = tensors[0];

		int wrappedDim = (int) TensorUtil.wrapDim(dim, first.shape().length);

		long[][] allShapes = new long[tensors.length][];

		int targetDimShape = 0;
		for (int i = 0; i < tensors.length; ++i) {
			allShapes[i] = tensors[i].shape();
			targetDimShape += allShapes[i][wrappedDim];
		}

		long[] shape = allShapes[0].clone();
		shape[wrappedDim] = targetDimShape;

		if (cattedTensor == null) {
			cattedTensor = TensorUtil.of(shape, first.type);
		}

		long[] outterCoords = new long[wrappedDim + 1];

		int outer = 1;
		for (int i = 0; i < wrappedDim; ++i) {
			outer *= shape[i];
		}

		CoordInc coordInc = new CoordInc(shape, wrappedDim, outterCoords);

		for (int i = 0; i < outer; ++i) {

			long[] innerCoords = outterCoords.clone();
			innerCoords[wrappedDim] = 0;

			for (int j = 0; j < tensors.length; ++j) {
				for (int z = 0; z < allShapes[j][wrappedDim]; ++z, innerCoords[wrappedDim] += 1) {
					outterCoords[wrappedDim] = z;

					cattedTensor.getData().set(tensors[j].getData().get(outterCoords), innerCoords);
				}
			}

			coordInc.inc();
		}

		return cattedTensor;
	}

	public static <DT, T extends Tensor <DT>>
	T permute(T tensor, long... dims) {
		return tensor;
	}

	Tensor(NdArray <DT> data, DataType type) {
		this.data = data;
		this.type = type;
	}

	public DataType getType() {
		return type;
	}

	NdArray <DT> getData() {
		return data;
	}

	abstract void parseFromValueStrings(String[] valueStrings);

	abstract String[] getValueStrings();

	@Override
	public String toDisplaySummary() {
		return type.toString().substring(0,1) + type.toString().substring(1).toLowerCase() + "Tensor("
			+ TensorUtil.toString(Shape.fromNdArrayShape(this.data.shape()))+")";
	}

	@Override
	public String toDisplayData(int n) {
		return PrettyDisplayUtils.displayTensor(shape(), getValueStrings(), n);
	}

	@Override
	public String toShortDisplayData() {
		return toDisplayData(3);
	}
}
