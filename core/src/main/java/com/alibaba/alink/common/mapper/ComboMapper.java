package com.alibaba.alink.common.mapper;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class ComboMapper extends Mapper {
	private static final Logger LOG = LoggerFactory.getLogger(ComboMapper.class);

	protected MapperChain mapperList;

	public ComboMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	public List <Mapper> getLoadedMapperList() {
		List <Mapper> mapperList = new ArrayList <>();
		if (this.mapperList == null || this.mapperList.getMappers() == null) {
			return null;
		}
		Collections.addAll(mapperList, this.mapperList.getMappers());
		return mapperList;
	}

	@Override
	public void open() {
		newMapperList();
		mapperList.open();
	}

	public void newMapperList() {
		if (mapperList == null) {
			List <Mapper> mapperList2 = getLoadedMapperList();
			if (mapperList2 != null) {
				mapperList = new MapperChain(mapperList2.toArray(new Mapper[0]));
			}
		}
	}

	@Override
	public void close() {
		if (mapperList != null) {
			mapperList.close();
		}
	}

	@Override
	public final Row map(Row row) throws Exception {
		return mapperList.map(row);
	}

	@Override
	public final void bufferMap(Row bufferRow, int[] bufferSelectedColIndices, int[] bufferResultColIndices)
		throws Exception {
		throw new Exception("ComboModelMapper not support bufferRow.");
	}

	@Override
	protected final void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		throw new RuntimeException("ComboModelMapper not support map slice!");
	}
}
