package com.alibaba.alink.operator.common.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkIllegalModelException;
import com.alibaba.alink.common.exceptions.AkIllegalOperationException;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.UnionAllBatchOp;
import com.alibaba.alink.pipeline.ModelExporterUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * The class is for package batch op list and unpack in local predictor
 */
public class PackBatchOperatorUtil {
	//model col prefix
	private static final String MODEL_COL_PREFIX = "p";
	//id col name
	private static final String ID_COL_NAME = "id";

	/**
	 * pack batch ops
	 */
	public static BatchOperator packBatchOps(BatchOperator <?>[] batchOps) {
		if (batchOps == null || batchOps.length == 0) {
			throw new AkIllegalOperationException("batchOps must be set.");
		}

		Tuple2 <TableSchema, List <int[]>> mergeTypesAndIndices = mergeTypes(batchOps);
		TableSchema outSchema = mergeTypesAndIndices.f0;
		List <int[]> colIndices = mergeTypesAndIndices.f1;

		List <BatchOperator <?>> packedOps = new ArrayList <>();
		packedOps.add(getPackMetaOp(batchOps, colIndices, outSchema));

		for (int i = 0; i < batchOps.length; i++) {
			packedOps.add(
				packBatchOp(batchOps[i],
					outSchema,
					i,
					colIndices.get(i)));
		}

		return new UnionAllBatchOp()
			.setMLEnvironmentId(batchOps[0].getMLEnvironmentId())
			.linkFrom(packedOps);
	}

	/**
	 * Given model data and selected op index, return row of batch op.
	 */
	public static List <Row> unpackRows(List <Row> modelRows, int selectedOpIndex) {
		Tuple2 <String[], int[]> colNamesAndIndices = getColNamesAndIndices(modelRows, selectedOpIndex);
		List <Row> output = new ArrayList <>();
		for (Row row : modelRows) {
			if (((Number) row.getField(0)).intValue() == selectedOpIndex) {
				output.add(Row.project(row, colNamesAndIndices.f1));
			}
		}
		return output;
	}

	/**
	 * Given model data, model schema and  selected op index, return schema of batch op.
	 */
	public static TableSchema unpackSchema(List <Row> modelRows,
										   TableSchema modelSchema,
										   int selectedOpIndex) {
		Tuple2 <String[], int[]> colNamesAndIndices = getColNamesAndIndices(modelRows, selectedOpIndex);
		String[] colNames = colNamesAndIndices.f0;
		int[] indices = colNamesAndIndices.f1;

		TypeInformation[] types = new TypeInformation[colNames.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = modelSchema.getFieldType(indices[i]).get();
		}

		return new TableSchema(colNames, types);
	}

	/**
	 * get col names and ncol indices.
	 */
	private static Tuple2 <String[], int[]> getColNamesAndIndices(List <Row> modelRows,
																  int selectedOpIndex) {
		for (Row row : modelRows) {
			if (((Number) row.getField(0)).intValue() == -1) {
				Tuple2 <List <List <String>>, List <List <Integer>>> metaData =
					JsonConverter.fromJson((String) row.getField(1), Tuple2.class);

				if (selectedOpIndex < 0 ||
					selectedOpIndex >= metaData.f1.size() ||
					selectedOpIndex >= metaData.f0.size()) {
					throw new AkIllegalModelException("selectedOpIndex outbound. "
						+ "selecedOpIndex is " + selectedOpIndex +
						". opNum: " + metaData.f0.size() +
						". indicesNum: " + metaData.f1.size());
				}

				List <Integer> indices = metaData.f1.get(selectedOpIndex);
				int[] colIndicesArray = new int[indices.size()];
				for (int i = 0; i < indices.size(); i++) {
					colIndicesArray[i] = indices.get(i);
				}
				return Tuple2.of(metaData.f0.get(selectedOpIndex).toArray(new String[0]), colIndicesArray);
			}
		}
		throw new AkIllegalModelException("model has not meta.");
	}

	/**
	 * @return f0 is col types after merge, f1 is col idx of op in f0.
	 */
	private static Tuple2 <TableSchema, List <int[]>> mergeTypes(BatchOperator <?>[] batchOps) {
		List <int[]> colIndices = new ArrayList <>();
		//first col is id, second col is meta.
		TypeInformation[] mergeOpTypes = new TypeInformation[] {Types.STRING};
		for (BatchOperator <?> batchOp : batchOps) {
			Tuple2 <TypeInformation <?>[], int[]> mergeTwoOpTypes = ModelExporterUtils.mergeType(mergeOpTypes,
				batchOp.getColTypes());
			mergeOpTypes = mergeTwoOpTypes.f0;
			colIndices.add(addOne(mergeTwoOpTypes.f1));
		}

		String[] colNames = new String[mergeOpTypes.length + 1];
		colNames[0] = ID_COL_NAME;
		for (int i = 0; i < mergeOpTypes.length; i++) {
			colNames[i + 1] = MODEL_COL_PREFIX + i;
		}
		TypeInformation[] colTypes = new TypeInformation[colNames.length];
		colTypes[0] = Types.LONG;
		System.arraycopy(mergeOpTypes, 0, colTypes, 1, mergeOpTypes.length);

		return Tuple2.of(new TableSchema(colNames, colTypes), colIndices);
	}

	private static int[] addOne(int[] vec) {
		for (int i = 0; i < vec.length; i++) {
			vec[i]++;
		}
		return vec;
	}

	/**
	 * construct packOp meta.
	 */
	private static BatchOperator <?> getPackMetaOp(BatchOperator <?>[] batchOps,
												   List <int[]> colIndices,
												   TableSchema schema) {
		List <String[]> colNames = new ArrayList <>();
		for (BatchOperator <?> batchOp : batchOps) {
			colNames.add(batchOp.getColNames());
		}

		String meta = JsonConverter.toJson(Tuple2.of(colNames, colIndices));

		Row row = new Row(schema.getFieldNames().length);
		row.setField(0, -1L);
		row.setField(1, meta);

		List <Row> rowData = new ArrayList <>();
		rowData.add(row);

		return new MemSourceBatchOp(rowData, schema)
			.setMLEnvironmentId(batchOps[0].getMLEnvironmentId());
	}

	private static BatchOperator <?> packBatchOp(BatchOperator <?> op,
												 TableSchema outSchema,
												 int opIdx,
												 int[] colIndices) {
		return BatchOperator.fromTable(DataSetConversionUtil
			.toTable(op.getMLEnvironmentId(),
				op.getDataSet()
					.map(new FlattenMap(outSchema.getFieldNames().length, opIdx, colIndices)),
				outSchema));
	}

	/**
	 * first entry is opIdx, second is meta, other fill with colIndices, others is null.
	 */
	private static class FlattenMap implements MapFunction <Row, Row> {
		private static final long serialVersionUID = -4502881391819047945L;
		private int colNum;
		private int opIdx;
		private int[] colIndices;

		FlattenMap(int colNum, int opIdx, int[] colIndices) {
			this.colNum = colNum;
			this.opIdx = opIdx;
			this.colIndices = colIndices;
		}

		@Override
		public Row map(Row value) throws Exception {
			Row row = new Row(this.colNum);
			row.setField(0, (long) this.opIdx);
			for (int i = 0; i < value.getArity(); i++) {
				row.setField(colIndices[i], value.getField(i));
			}
			return row;
		}
	}

}