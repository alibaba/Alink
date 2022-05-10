package com.alibaba.alink.operator.common.timeseries;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.timeseries.LookupVectorInTimeSeriesParams;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class LookupVectorInTimeSeriesMapper extends Mapper {

	public LookupVectorInTimeSeriesMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);

		String timeCol = params.get(LookupVectorInTimeSeriesParams.TIME_COL);
		TypeInformation<?> typeTime = TableUtil.findColType(dataSchema, timeCol);
		if (Types.SQL_TIMESTAMP != typeTime) {
			throw new IllegalArgumentException("Type of column '" + timeCol + "' must be timestamp!");
		}

		String timeSeriesCol = params.get(LookupVectorInTimeSeriesParams.TIME_SERIES_COL);
		TypeInformation<?> typeTS = TableUtil.findColType(dataSchema, timeSeriesCol);
		if (!AlinkTypes.M_TABLE.equals(typeTS) && Types.STRING != typeTime) {
			throw new IllegalArgumentException("Type of column '" + timeSeriesCol + "' must be MTable!");
		}
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		MTable mTable = null;
		if (selection.get(1) == null) {
			result.set(0, null);
			return;
		}
		if (selection.get(1) instanceof MTable) {
			mTable = (MTable) selection.get(1);
		} else {
			mTable = MTable.fromJson((String) selection.get(1));
		}

		if (mTable.getNumRow() == 0) {
			result.set(0, null);
			return;
		}

		Timestamp lookupTime = (Timestamp) selection.get(0);

		TableSchema schema = mTable.getSchema();
		String timeCol = null;
		String vectorCol = null;
		int timeIdx = -1;
		TypeInformation<?>[] colTypes = schema.getFieldTypes();
		for (int i = 0; i < colTypes.length; i++) {
			if (colTypes[i] == Types.SQL_TIMESTAMP) {
				timeCol = schema.getFieldNames()[i];
				timeIdx = i;
			}
			if (colTypes[i] == AlinkTypes.VECTOR || colTypes[i] == AlinkTypes.DENSE_VECTOR
					|| colTypes[i] == AlinkTypes.SPARSE_VECTOR) {
				vectorCol = schema.getFieldNames()[i];
			}
		}

		if (null != timeCol && null != vectorCol) {
			List<Object> times = MTableUtil.getColumn(mTable, timeCol);
			int idxRow = times.indexOf(lookupTime);
			int idxCol = TableUtil.findColIndex(schema, vectorCol);
			if (idxRow >= 0) {
				result.set(0, mTable.getEntry(idxRow, idxCol));
				return;
			} else {
				mTable.orderBy(timeIdx);
				Timestamp[] timesArr = MTableUtil.getColumn(mTable, timeCol).toArray(new Timestamp[]{});
				int pos = Arrays.binarySearch(timesArr, lookupTime);
				if (pos == -1) {
					result.set(0, mTable.getEntry(0, idxCol));
				} else if (-pos == timesArr.length + 1) {
					result.set(0, mTable.getEntry(timesArr.length - 1, idxCol));
				} else {
					int pos0 = -2 - pos;
					int pos1 = -1 - pos;
					long time0 = timesArr[pos0].getTime();
					long time1 = timesArr[pos1].getTime();
					double scale = (double) (lookupTime.getTime() - time0) / (double) (time1 - time0);
					DenseVector inter = ((DenseVector) mTable.getEntry(pos0, idxCol)).scale(1 - scale);
					inter.plusEqual(((DenseVector) mTable.getEntry(pos1, idxCol)).scale(scale));
					result.set(0, inter);
				}
				return;
			}
		}

		result.set(0, null);

	}

	@Override
	protected Tuple4<String[], String[], TypeInformation<?>[], String[]> prepareIoSchema(TableSchema dataSchema,
	                                                                                     Params params) {
		return new Tuple4<>(
				new String[]{
						params.get(LookupVectorInTimeSeriesParams.TIME_COL),
						params.get(LookupVectorInTimeSeriesParams.TIME_SERIES_COL),
				},
				new String[]{params.get(LookupVectorInTimeSeriesParams.OUTPUT_COL)},
				new TypeInformation<?>[]{AlinkTypes.VECTOR},
				params.get(LookupVectorInTimeSeriesParams.RESERVED_COLS)
		);
	}
}
