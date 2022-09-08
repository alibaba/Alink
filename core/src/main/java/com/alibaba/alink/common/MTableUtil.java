package com.alibaba.alink.common;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.local.AlinkLocalSession;
import com.alibaba.alink.operator.local.AlinkLocalSession.TaskRunner;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.shared.HasNumThreads;

import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class MTableUtil implements Serializable {

	public static List <Object> getColumn(MTable mTable, String colName) {
		String[] names = mTable.getColNames();
		int idx = TableUtil.findColIndex(names, colName);
		if (idx == -1) {
			return null;
		}
		List <Row> table = mTable.getRows();
		List <Object> ret = new ArrayList <>(table.size());
		for (int i = 0; i < table.size(); ++i) {
			ret.add(i, table.get(i).getField(idx));
		}
		return ret;
	}

	public static Map <String, List <Object>> getColumns(MTable mTable) {
		List <Row> table = mTable.getRows();
		String[] names = mTable.getColNames();
		Map <String, List <Object>> ret = new HashMap <>(names.length);
		for (String name : names) {
			int idx = TableUtil.findColIndex(names, name);
			List <Object> objects = new ArrayList <>(table.size());
			for (int i = 0; i < table.size(); ++i) {
				objects.add(i, table.get(i).getField(idx));
			}
			ret.put(name, objects);
		}
		return ret;
	}

	public static MTable getMTable(Object obj) {
		if (obj == null) {
			return null;
		}
		if (obj instanceof MTable) {
			return (MTable) obj;
		} else if (obj instanceof String) {
			return MTable.fromJson((String) obj);
		} else {
			throw new AkIllegalDataException("Type must be string or mtable");
		}

	}

	static MTable copy(MTable mt) {
		ArrayList <Row> rows = new ArrayList <>(mt.getRows().size());
		for (Row r : mt.getRows()) {
			rows.add(Row.copy(r));
		}
		return new MTable(rows, mt.getSchemaStr());
	}

	static MTable select(MTable mt, String[] colNames) {
		return select(mt, colNames, TableUtil.findColIndicesWithAssertAndHint(mt.getSchema(), colNames));
	}

	static MTable select(MTable mt, int[] colIndexes) {
		String[] colAllNames = mt.getColNames();
		String[] colNames = new String[colIndexes.length];
		for (int i = 0; i < colIndexes.length; i++) {
			colNames[i] = colAllNames[colIndexes[i]];
		}
		return select(mt, colNames, colIndexes);
	}

	private static MTable select(MTable mt, String[] colNames, int[] colIndexes) {
		ArrayList <Row> rows = new ArrayList <>();
		for (Row r : mt.getRows()) {
			rows.add(Row.project(r, colIndexes));
		}
		return new MTable(rows, colNames, TableUtil.findColTypesWithAssertAndHint(mt.getSchema(), colNames));
	}

	public static List <Row> groupFunc(MTable mt, String[] groupCols, GroupFunction func) {
		final int[] groupColIndexes = TableUtil.findColIndicesWithAssertAndHint(mt.getSchema(), groupCols);
		final TypeComparator <Row> comparator = new RowTypeInfo(mt.getColTypes(), mt.getColNames())
			.createComparator(groupColIndexes, new boolean[groupColIndexes.length], 0, new ExecutionConfig());

		List <Row> list = new ArrayList <>();
		list.addAll(mt.getRows());
		MTable ordered = new MTable(list, mt.getSchemaStr());
		ordered.orderBy(groupCols);

		RowCollector rowCollector = new RowCollector();
		list = ordered.getRows();
		int start = 0;
		while (start < list.size()) {
			int end;
			Row rowStart = list.get(start);
			for (end = start + 1; end < list.size(); end++) {
				if (comparator.compare(rowStart, list.get(end)) != 0) {
					break;
				}
			}
			func.calc(list.subList(start, end), rowCollector);
			start = end;
		}
		return rowCollector.getRows();
	}

	public interface GroupFunction extends Function, Serializable {
		void calc(List <Row> values, Collector <Row> out);

		@Override
		default Object apply(Object o) {
			return null;
		}
	}

	public static List <Row> flatMapWithMultiThreads(MTable mt, Params params, FlatMapFunction flatMapFunction) {
		int numThreads = LocalOperator.getDefaultNumThreads();
		if (params.contains(HasNumThreads.NUM_THREADS)) {
			numThreads = params.get(HasNumThreads.NUM_THREADS);
		}
		return flatMapWithMultiThreads(mt, numThreads, flatMapFunction);
	}

	public static List <Row> flatMapWithMultiThreads(MTable mt, int numThreads, FlatMapFunction flatMapFunction) {
		return flatMapWithMultiThreads(mt.getRows(), numThreads, flatMapFunction);
	}

	public static List <Row> flatMapWithMultiThreads(List<Row> mt, Params params, FlatMapFunction flatMapFunction) {
		int numThreads = LocalOperator.getDefaultNumThreads();
		if (params.contains(HasNumThreads.NUM_THREADS)) {
			numThreads = params.get(HasNumThreads.NUM_THREADS);
		}
		return flatMapWithMultiThreads(mt, numThreads, flatMapFunction);
	}

	public static List <Row> flatMapWithMultiThreads(List<Row> mt, int numThreads, FlatMapFunction flatMapFunction) {
		final TaskRunner taskRunner = new TaskRunner();

		final List <Row>[] buffer = new List[numThreads];

		final int numRows = mt.size();

		final byte[] serialized = SerializationUtils.serialize(flatMapFunction);

		for (int i = 0; i < numThreads; ++i) {
			final int start = (int) AlinkLocalSession.DISTRIBUTOR.startPos(i, numThreads, numRows);
			final int cnt = (int) AlinkLocalSession.DISTRIBUTOR.localRowCnt(i, numThreads, numRows);
			final int curThread = i;

			if (cnt <= 0) {continue;}

			taskRunner.submit(() -> {
					FlatMapFunction localFlatMapFunction = SerializationUtils.deserialize(serialized);
					RowCollector rowCollector = new RowCollector();
					for (int j = start; j < start + cnt; j++) {
						try {
							localFlatMapFunction.flatMap(mt.get(j), rowCollector);
						} catch (Exception e) {
							throw new AkIllegalDataException("FlatMap error on the data : " + mt.get(j).toString(), e);
						}
					}
					buffer[curThread] = rowCollector.getRows();
				}
			);
		}

		taskRunner.join();

		ArrayList <Row> output = new ArrayList <>();
		for (int i = 0; i < numThreads; ++i) {
			if (buffer[i] != null) {
				output.addAll(buffer[i]);
			}
		}
		return output;
	}

	public interface FlatMapFunction extends Function, Serializable {
		void flatMap(Row row, Collector <Row> collector) throws Exception;

		@Override
		default Object apply(Object o) {
			return null;
		}
	}

	public static <I, O> List <O> flatMapWithMultiThreads(List<I> mt, Params params, GenericFlatMapFunction<I, O> flatMapFunction) {
		int numThreads = LocalOperator.getDefaultNumThreads();
		if (params.contains(HasNumThreads.NUM_THREADS)) {
			numThreads = params.get(HasNumThreads.NUM_THREADS);
		}
		return flatMapWithMultiThreads(mt, numThreads, flatMapFunction);
	}
	public static <I, O>  List <O> flatMapWithMultiThreads(
		List<I> mt, int numThreads, GenericFlatMapFunction<I, O> flatMapFunction) {

		final TaskRunner taskRunner = new TaskRunner();

		final List <O>[] buffer = new List[numThreads];

		final int numRows = mt.size();

		final byte[] serialized = SerializationUtils.serialize(flatMapFunction);

		for (int i = 0; i < numThreads; ++i) {
			final int start = (int) AlinkLocalSession.DISTRIBUTOR.startPos(i, numThreads, numRows);
			final int cnt = (int) AlinkLocalSession.DISTRIBUTOR.localRowCnt(i, numThreads, numRows);
			final int curThread = i;

			if (cnt <= 0) {continue;}

			taskRunner.submit(() -> {

					GenericFlatMapFunction<I, O> localFlatMapFunction = SerializationUtils.deserialize(serialized);

					final List<O> output = new ArrayList <>();

					for (int j = start; j < start + cnt; j++) {
						try {
							localFlatMapFunction.flatMap(mt.get(j), new Collector <O>() {
								@Override
								public void collect(O record) {
									output.add(record);
								}

								@Override
								public void close() {

								}
							});
						} catch (Exception e) {
							throw new AkIllegalDataException("FlatMap error on the data : " + mt.get(j).toString());
						}
					}

					buffer[curThread] = output;
				}
			);
		}

		taskRunner.join();

		ArrayList <O> output = new ArrayList <>();
		for (int i = 0; i < numThreads; ++i) {
			if (buffer[i] != null) {
				output.addAll(buffer[i]);
			}
		}
		return output;
	}

	public interface GenericFlatMapFunction<I, O> extends Serializable {
		void flatMap(I input, Collector <O> collector) throws Exception;
	}
}
