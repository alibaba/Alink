package com.alibaba.alink.common.linalg.tensor;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import org.tensorflow.ndarray.BooleanNdArray;
import org.tensorflow.ndarray.ByteNdArray;
import org.tensorflow.ndarray.DoubleNdArray;
import org.tensorflow.ndarray.FloatNdArray;
import org.tensorflow.ndarray.IntNdArray;
import org.tensorflow.ndarray.LongNdArray;
import org.tensorflow.ndarray.NdArray;
import org.tensorflow.ndarray.NdArrays;
import org.tensorflow.ndarray.Shape;
import org.tensorflow.ndarray.buffer.BooleanDataBuffer;
import org.tensorflow.ndarray.buffer.ByteDataBuffer;
import org.tensorflow.ndarray.buffer.DataBuffer;
import org.tensorflow.ndarray.buffer.DataBuffers;
import org.tensorflow.ndarray.buffer.DoubleDataBuffer;
import org.tensorflow.ndarray.buffer.FloatDataBuffer;
import org.tensorflow.ndarray.buffer.IntDataBuffer;
import org.tensorflow.ndarray.buffer.LongDataBuffer;
import org.tensorflow.ndarray.impl.buffer.misc.MiscDataBufferFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

/**
 * This class is only for internal usage, which provides access for some protected members and methods of {@link Tensor}
 * for classes in plugin.
 */
@Internal
public class TensorInternalUtils {
	public static <DT> NdArray <DT> getTensorData(Tensor <DT> tensor) {
		return tensor.getData();
	}

	public static FloatTensor createFloatTensor(FloatNdArray array) {
		return new FloatTensor(array);
	}

	public static DoubleTensor createDoubleTensor(DoubleNdArray array) {
		return new DoubleTensor(array);
	}

	public static IntTensor createIntTensor(IntNdArray array) {
		return new IntTensor(array);
	}

	public static LongTensor createLongTensor(LongNdArray array) {
		return new LongTensor(array);
	}

	public static BoolTensor createBoolTensor(BooleanNdArray array) {
		return new BoolTensor(array);
	}

	public static ByteTensor createByteTensor(ByteNdArray array) {
		return new ByteTensor(array);
	}

	public static UByteTensor createUByteTensor(ByteNdArray array) {
		return new UByteTensor(array);
	}

	public static StringTensor createStringTensor(NdArray <String> array) {
		return new StringTensor(array);
	}

	public static String[] getValueStrs(Tensor <?> tensor) {
		return tensor.getValueStrings();
	}

	public static Tensor <?> bytesToTensor(byte[] bytes, DataType dataType, org.tensorflow.ndarray.Shape shape) {
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
		switch (dataType) {
			case FLOAT:
				FloatNdArray floatNdArray = NdArrays.ofFloats(org.tensorflow.ndarray.Shape.of(shape.asArray()));
				floatNdArray.write(DataBuffers.of(byteBuffer.asFloatBuffer()));
				return createFloatTensor(floatNdArray);
			case DOUBLE:
				DoubleNdArray doubleNdArray = NdArrays.ofDoubles(org.tensorflow.ndarray.Shape.of(shape.asArray()));
				doubleNdArray.write(DataBuffers.of(byteBuffer.asDoubleBuffer()));
				return createDoubleTensor(doubleNdArray);
			case INT:
				IntNdArray intNdArray = NdArrays.ofInts(org.tensorflow.ndarray.Shape.of(shape.asArray()));
				intNdArray.write(DataBuffers.of(byteBuffer.asIntBuffer()));
				return createIntTensor(intNdArray);
			case LONG:
				LongNdArray longNdArray = NdArrays.ofLongs(org.tensorflow.ndarray.Shape.of(shape.asArray()));
				longNdArray.write(DataBuffers.of(byteBuffer.asLongBuffer()));
				return createLongTensor(longNdArray);
			case BYTE: {
				ByteNdArray byteNdArray = NdArrays.ofBytes(Shape.of(shape.asArray()));
				byteNdArray.write(DataBuffers.of(byteBuffer));
				return createByteTensor(byteNdArray);
			}
			case UBYTE: {
				ByteNdArray byteNdArray = NdArrays.ofBytes(Shape.of(shape.asArray()));
				byteNdArray.write(DataBuffers.of(byteBuffer));
				return createUByteTensor(byteNdArray);
			}
			case BOOLEAN:
				BooleanNdArray booleanNdArray = NdArrays.ofBooleans(Shape.of(shape.asArray()));
				BitSet bitSet = BitSet.valueOf(byteBuffer);
				BooleanDataBuffer booleanDataBuffer = MiscDataBufferFactory.create(bitSet, shape.size(), true);
				booleanNdArray.write(booleanDataBuffer);
				return createBoolTensor(booleanNdArray);
			case STRING: {
				int size = Math.toIntExact(shape.size());
				String[] strings = new String[size];
				NdArray <String> stringNdArray = NdArrays.ofObjects(String.class, Shape.of(shape.asArray()));
				try (
					ByteArrayInputStream bais = new ByteArrayInputStream(byteBuffer.array());
					DataInputStream dis = new DataInputStream(bais)) {
					int sizeRead = dis.readInt();
					if (sizeRead != size) {
						throw new AkUnclassifiedErrorException(
							String.format("Size %d in bytes are different from computed value %d from shape.", sizeRead,
								size));
					}
					for (int i = 0; i < size; i += 1) {
						int len = dis.readInt();
						byte[] b = new byte[len];
						dis.readFully(b);
						strings[i] = new String(b, StandardCharsets.UTF_8);
					}
				} catch (IOException e) {
					throw new AkUnclassifiedErrorException("Cannot deserialize bytes to a StringTensor:", e);
				}
				DataBuffer <String> stringDataBuffer = DataBuffers.of(strings, false, false);
				stringNdArray.write(stringDataBuffer);
				return createStringTensor(stringNdArray);
			}
			default:
				throw new AkUnsupportedOperationException("Unsupported tensor type " + dataType);
		}
	}

	public static byte[] tensorToBytes(Tensor <?> tensor) {
		int size = (int) tensor.size();
		if (tensor instanceof FloatTensor) {
			NdArray <Float> tensorData = getTensorData((FloatTensor) tensor);
			ByteBuffer buffer = ByteBuffer.allocate(size * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
			FloatDataBuffer floatDataBuffer = DataBuffers.of(buffer.asFloatBuffer());
			tensorData.read(floatDataBuffer);
			return buffer.array();
		} else if (tensor instanceof DoubleTensor) {
			NdArray <Double> tensorData = getTensorData((DoubleTensor) tensor);
			ByteBuffer buffer = ByteBuffer.allocate(size * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
			DoubleDataBuffer doubleDataBuffer = DataBuffers.of(buffer.asDoubleBuffer());
			tensorData.read(doubleDataBuffer);
			return buffer.array();
		} else if (tensor instanceof IntTensor) {
			NdArray <Integer> tensorData = getTensorData((IntTensor) tensor);
			ByteBuffer buffer = ByteBuffer.allocate(size * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
			IntDataBuffer intDataBuffer = DataBuffers.of(buffer.asIntBuffer());
			tensorData.read(intDataBuffer);
			return buffer.array();
		} else if (tensor instanceof LongTensor) {
			NdArray <Long> tensorData = getTensorData((LongTensor) tensor);
			ByteBuffer buffer = ByteBuffer.allocate(size * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
			LongDataBuffer longDataBuffer = DataBuffers.of(buffer.asLongBuffer());
			tensorData.read(longDataBuffer);
			return buffer.array();
		} else if (tensor instanceof ByteTensor) {
			NdArray <Byte> tensorData = getTensorData((ByteTensor) tensor);
			ByteBuffer buffer = ByteBuffer.allocate(size * Byte.BYTES).order(ByteOrder.LITTLE_ENDIAN);
			ByteDataBuffer byteDataBuffer = DataBuffers.of(buffer);
			tensorData.read(byteDataBuffer);
			return buffer.array();
		} else if (tensor instanceof UByteTensor) {
			NdArray <Byte> tensorData = getTensorData((UByteTensor) tensor);
			ByteBuffer buffer = ByteBuffer.allocate(size * Byte.BYTES).order(ByteOrder.LITTLE_ENDIAN);
			ByteDataBuffer byteDataBuffer = DataBuffers.of(buffer);
			tensorData.read(byteDataBuffer);
			return buffer.array();
		} else if (tensor instanceof BoolTensor) {
			NdArray <Boolean> tensorData = getTensorData((BoolTensor) tensor);
			int nbits = Math.toIntExact(tensorData.size());
			BitSet bitSet = new BitSet(nbits);
			BooleanDataBuffer booleanDataBuffer = MiscDataBufferFactory.create(bitSet, nbits, false);
			tensorData.read(booleanDataBuffer);
			return bitSet.toByteArray();
		} else if (tensor instanceof StringTensor) {
			NdArray <String> tensorData = getTensorData((StringTensor) tensor);
			String[] strings = new String[size];
			DataBuffer <String> stringDataBuffer = DataBuffers.of(strings, false, false);
			tensorData.read(stringDataBuffer);
			try (
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(baos)) {
				dos.writeInt(size);
				for (String string : strings) {
					byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
					dos.writeInt(bytes.length);
					dos.write(bytes);
				}
				return baos.toByteArray();
			} catch (IOException e) {
				throw new AkUnclassifiedErrorException("Cannot serialize StringTensor to bytes:", e);
			}
		} else {
			throw new AkUnsupportedOperationException("Unsupported tensor type: " + tensor.getClass().getName());
		}
	}
}
