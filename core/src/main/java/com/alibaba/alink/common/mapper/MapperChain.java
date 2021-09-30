package com.alibaba.alink.common.mapper;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.TableUtil;
import com.sun.org.apache.xpath.internal.operations.Mod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * a chain of mapper
 */
public class MapperChain {

	private static final Logger LOG = LoggerFactory.getLogger(MapperChain.class);

	private Mapper[] mappers;
	private List <int[]> mapperSelectedColIndices;
	private List <int[]> mapperResultColIndices;
	private int[] outputColIndices;
	private transient ThreadLocal <Row> threadBufferRow;
	private boolean isOneMapper = false;
	private int bufferRowColNum;

	public MapperChain(Mapper[] mappers) {
		this.mappers = mappers;
		List <Mapper> flattened = new ArrayList <>();
		expandMappers(this.mappers, flattened);
		this.mappers = flattened.toArray(new Mapper[0]);
		if (this.mappers.length == 1) {
			isOneMapper = true;
		} else {
			getInOutIndices();
			this.threadBufferRow = ThreadLocal.withInitial(() -> new Row(bufferRowColNum));
		}
	}

	public void open() {
		for (Mapper mapper : mappers) {
			mapper.open();
		}
	}

	public void close() {
		for (Mapper mapper : mappers) {
			mapper.close();
		}
	}

	public TableSchema getOutTableSchema() {
		return this.mappers[this.mappers.length - 1].getOutputSchema();
	}

	public Row map(Row row) throws Exception {
		boolean isModelStream = false;
		for (Mapper mapper : this.mappers) {
			if (mapper instanceof ComboModelMapper) {
				if (ModelStreamModelMapperAdapt.useModelStreamFile((ModelMapper) mapper)) {
					isModelStream = true;
					break;
				}
			}
		}
		if (isModelStream) {
			Row out = row;
			for (int i = 0; i < mappers.length; i++) {
				out = mappers[i].map(out);
			}
			return Row.project(out, this.outputColIndices);
		} else if (isOneMapper) {
			return mappers[0].map(row);
		} else {
			Row out = this.threadBufferRow.get();
			for (int i = 0; i < row.getArity(); i++) {
				out.setField(i, row.getField(i));
			}
			for (int i = 0; i < mappers.length; i++) {
				mappers[i].bufferMap(out, mapperSelectedColIndices.get(i), mapperResultColIndices.get(i));
			}
			return Row.project(out, this.outputColIndices);
		}
	}

	public Mapper[] getMappers() {
		return this.mappers;
	}

	private String[] mergeCols() {
		Set <String> mergeColNames = new LinkedHashSet <>();
		Collections.addAll(mergeColNames, mappers[0].getDataSchema().getFieldNames());
		for (Mapper mapper : this.mappers) {
			Collections.addAll(mergeColNames, mapper.getSelectedCols());
			Collections.addAll(mergeColNames, mapper.getResultCols());
		}
		return mergeColNames.toArray(new String[0]);
	}

	private void getInOutIndices() {
		if (this.mappers.length > 0) {
			String[] bufferRowColNames = mergeCols();
			this.bufferRowColNum = bufferRowColNames.length;
			this.mapperSelectedColIndices = new ArrayList <>();
			this.mapperResultColIndices = new ArrayList <>();

			for (Mapper mapper : this.mappers) {
				this.mapperSelectedColIndices.add(
					TableUtil.findColIndices(bufferRowColNames, mapper.getSelectedCols()));
				this.mapperResultColIndices.add(
					TableUtil.findColIndices(bufferRowColNames, mapper.getResultCols()));
			}

			this.outputColIndices = TableUtil.findColIndices(bufferRowColNames,
				mappers[mappers.length - 1].getOutputSchema().getFieldNames());

		}
	}

	private static void expandMappers(Mapper[] mappers, List <Mapper> flattened) {
		for (Mapper mapper : mappers) {
			if (mapper instanceof ComboModelMapper && !ModelStreamModelMapperAdapt.useModelStreamFile(
				(ModelMapper) mapper)) {
				if (((ComboModelMapper) mapper).mapperList != null) {
					Mapper[] innerMappers = ((ComboModelMapper) mapper).mapperList.mappers;
					if (innerMappers != null) {
						expandMappers(innerMappers, flattened);
					}
				}
			} else {
				if (mapper instanceof ModelMapper
					&& ModelStreamModelMapperAdapt.useModelStreamFile((ModelMapper) mapper)) {
					mapper = new ModelStreamModelMapperAdapt((ModelMapper) mapper);
				}
				flattened.add(mapper);
			}
		}
		//todo: deal with numThread and lazy
	}

}
