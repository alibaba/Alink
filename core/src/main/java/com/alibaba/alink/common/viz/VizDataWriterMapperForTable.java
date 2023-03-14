package com.alibaba.alink.common.viz;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.utils.AlinkSerializable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

/**
 * Use `kv` to write `table` data.
 * Writing `table` data directly could be un-parsable due to special characters in tables, like `,`, `\n`, and so on.
 */
public class VizDataWriterMapperForTable implements MapPartitionFunction <Row, String> {
	private static final long serialVersionUID = -8765896344566463568L;
	VizDataWriterInterface writer;
	String[] colNames;
	TypeInformation[] colTypes;
	int dataId;

	VizDataWriterMapperForTable(VizDataWriterInterface writer, int dataId, String[] colNames,
								TypeInformation[] colTypes) {
		this.writer = writer;
		this.colNames = colNames;
		this.colTypes = colTypes;
		this.dataId = dataId;
	}

	@Override
	public void mapPartition(Iterable <Row> iterable, Collector <String> collector) throws Exception {
		Table result = new Table();
		result.colNamesJson = gson.toJson(colNames);
		result.colTypesJson = gson.toJson(Arrays.stream(colTypes).map(d -> d.toString()).collect(Collectors.toList()));
		for (Row row : iterable) {
			int n = row.getArity();
			Object[] fields = new Object[n];
			for (int i = 0; i < n; i += 1) {
				fields[i] = row.getField(i);
			}
			result.fieldsJson.add(gson.toJson(fields));
		}
		String resultJson = gson.toJson(result);
		//        System.err.println(resultJson);
		writer.writeBatchData(this.dataId, resultJson, System.currentTimeMillis());
	}

	public static class Table implements AlinkSerializable {
		public String colNamesJson;
		public String colTypesJson;
		public List <String> fieldsJson = new ArrayList <>();
	}
}
