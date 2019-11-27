package com.alibaba.alink.common.utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Test cases for VectorTypes.
 */
public class VectorTypesTest {

	@Test
	public void testGetTypeName() {
		Assert.assertEquals("VEC_TYPES_DENSE_VECTOR", VectorTypes.getTypeName(TypeInformation.of(DenseVector.class)));
		Assert.assertEquals("VEC_TYPES_SPARSE_VECTOR", VectorTypes.getTypeName(TypeInformation.of(SparseVector.class)));
		Assert.assertEquals("VEC_TYPES_VECTOR", VectorTypes.getTypeName(TypeInformation.of(Vector.class)));
	}

	@Test
	public void testGetTypeInformation() {
		Assert.assertEquals(VectorTypes.getTypeInformation("VEC_TYPES_DENSE_VECTOR"), TypeInformation.of(DenseVector.class));
		Assert.assertEquals(VectorTypes.getTypeInformation("VEC_TYPES_SPARSE_VECTOR"), TypeInformation.of(SparseVector.class));
		Assert.assertEquals(VectorTypes.getTypeInformation("VEC_TYPES_VECTOR"), TypeInformation.of(Vector.class));
	}

	@SuppressWarnings("unchecked")
	private static <V extends Vector> void doVectorSerDeserTest(TypeSerializer ser, V vector) throws IOException {
		DataOutputSerializer out = new DataOutputSerializer(1024);
		ser.serialize(vector, out);
		DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
		Vector deserialize = (Vector) ser.deserialize(in);
		Assert.assertEquals(vector.getClass(), deserialize.getClass());
		Assert.assertEquals(vector, deserialize);
	}

	@Test
	public void testVectorsSerDeser() throws IOException {
		// Prepare data
		SparseVector sparseVector = new SparseVector(10, new HashMap<Integer, Double>() {{
			ThreadLocalRandom rand = ThreadLocalRandom.current();
			for (int i = 0; i < 10; i += 2) {
				this.put(i, rand.nextDouble());
			}
		}});
		DenseVector denseVector = DenseVector.rand(10);

		// Prepare serializer
		ExecutionConfig config = new ExecutionConfig();
		TypeSerializer<Vector> vecSer = VectorTypes.VECTOR.createSerializer(config);
		TypeSerializer<SparseVector> sparseSer = VectorTypes.SPARSE_VECTOR.createSerializer(config);
		TypeSerializer<DenseVector> denseSer = VectorTypes.DENSE_VECTOR.createSerializer(config);

		// Do tests.
		doVectorSerDeserTest(vecSer, sparseVector);
		doVectorSerDeserTest(vecSer, denseVector);
		doVectorSerDeserTest(sparseSer, sparseVector);
		doVectorSerDeserTest(denseSer, denseVector);
	}
}