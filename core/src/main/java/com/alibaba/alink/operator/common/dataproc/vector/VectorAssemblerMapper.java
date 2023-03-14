package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.MISOMapper;
import com.alibaba.alink.params.dataproc.vector.VectorAssemblerParams;
import com.alibaba.alink.params.shared.HasHandleInvalid.HandleInvalidMethod;

import java.util.HashMap;
import java.util.Map;

/**
 * This mapper maps many columns to one vector. the columns should be vector or numerical columns.
 */
public class VectorAssemblerMapper extends MISOMapper {
	/**
	 * this variable using to judge the output vector format.
	 * if nnz * ratio > vector size, the vector format is denseVector, else sparseVector.
	 * <p>
	 * if number of values in the vector is less than 1/RATIO of its length, then use SparseVector.
	 * else use DenseVector.
	 */
	private static final double RATIO = 1.5;
	private static final long serialVersionUID = -8419340084734506661L;

	/**
	 * the way to handle invalid input.
	 */
	private final HandleInvalidMethod handleInvalid;

	public VectorAssemblerMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.handleInvalid = params.get(VectorAssemblerParams.HANDLE_INVALID);
	}

	@Override
	protected TypeInformation<?> initOutputColType() {
		return AlinkTypes.VECTOR;
	}

	/**
	 * each column of the input may be a number or a Vector.
	 */
	@Override
	protected Object map(Object[] input) {
		return assembler(input, handleInvalid);
	}

	public static Object assembler(Object[] input) {
		return assembler(input, HandleInvalidMethod.ERROR);
	}

	private static Object assembler(Object[] input, HandleInvalidMethod handleInvalid) {
		if (null == input) {
			return null;
		}
		int pos = 0;

		int size = input.length;
		for (Object col : input) {
			if (col instanceof DenseVector) {
				size += ((DenseVector) col).size();
			} else if (col instanceof SparseVector) {
				size += ((SparseVector) col).getIndices().length;
			}
		}

		Map<Integer, Double> map = new HashMap<>(size);
		// getVector the data, and write it in List.
		for (Object col : input) {
			if (null != col) {
				if (col instanceof Number) {
					map.put(pos++, ((Number) col).doubleValue());
				} else if (col instanceof String) {
					Vector vec = VectorUtil.getVector(col);
					pos = appendVector(vec, map, pos);
				} else if (col instanceof Vector) {
					pos = appendVector((Vector) col, map, pos);
				} else {
					throw new AkUnsupportedOperationException("only support number, string and vector, other types will cause exception");
				}
			} else {
				switch (handleInvalid) {
					case ERROR:
						throw new AkIllegalDataException("null value is found in vector assembler inputs.");
					case SKIP:
						return null;
					default:
				}
			}
		}

		/* form the vector, and finally toString it. */
		Vector vec = new SparseVector(pos, map);

		if (map.size() * RATIO > pos) {
			vec = ((SparseVector)vec).toDenseVector();
		}
		return vec;
	}

	private static int appendVector(Vector vec, Map <Integer, Double> map, int pos) {
		if (vec instanceof SparseVector) {
			SparseVector sv = (SparseVector) vec;
			if (sv.size() <= 0) {
				throw new AkIllegalDataException("The append sparse vector must have size.");
			}
			int[] idx = sv.getIndices();
			double[] values = sv.getValues();
			for (int j = 0; j < idx.length; ++j) {
				map.put(pos + idx[j], values[j]);
			}
			pos += sv.size();
		} else if (vec instanceof DenseVector) {
			DenseVector dv = (DenseVector) vec;
			for (int j = 0; j < dv.size(); ++j) {
				map.put(pos++, dv.get(j));
			}
		}
		return pos;
	}
}
