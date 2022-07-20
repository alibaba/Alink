package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dataproc.LookupParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Model Mapper for Key to Values operation.
 */
public class LookupModelMapper extends ModelMapper {

	protected final int[] selectedColIndices;
	private final int[] mapKeyColIndices;
	private final int[] mapValueColIndices;

	private HashMap <List <Object>, Object[]> mapModel;
	private String[] outputColNames;
	private final int mk;
	private final List <Object> currentKey;

	public LookupModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);

		String[] mapKeyColNames = params.get(LookupParams.MAP_KEY_COLS);

		this.mapKeyColIndices = (mapKeyColNames != null) ?
			TableUtil.findColIndicesWithAssertAndHint(modelSchema, mapKeyColNames) : new int[] {0};
		String[] mapValueColNames = params.get(LookupParams.MAP_VALUE_COLS);
		this.mapValueColIndices = (mapValueColNames != null) ?
			TableUtil.findColIndicesWithAssertAndHint(modelSchema, mapValueColNames) : new int[] {1};
		if (modelSchema.getFieldNames().length > 2) {
			if (mapKeyColNames == null || mapValueColNames == null) {
				throw new AkIllegalOperatorParameterException("LookUpMapper err : mapKeyCols and mapValueCols should set in parameters.");
			}
		}
		this.mk = this.mapKeyColIndices.length;
		this.currentKey = new ArrayList <>(mk);
		for (int i = 0; i < mk; ++i) {
			this.currentKey.add(null);
		}

		String[] selectedColNames = params.get(LookupParams.SELECTED_COLS);
		this.selectedColIndices = TableUtil.findColIndicesWithAssertAndHint(dataSchema, selectedColNames);

		for (int i = 0; i < selectedColNames.length; ++i) {
			if (mapKeyColNames != null && mapValueColNames != null) {
				if (TableUtil.findColTypeWithAssertAndHint(dataSchema, selectedColNames[i])
					!= TableUtil.findColTypeWithAssertAndHint(modelSchema, mapKeyColNames[i])) {
					throw new AkIllegalDataException("Data types are not match. selected column type is "
						+ TableUtil.findColTypeWithAssertAndHint(dataSchema, selectedColNames[i])
						+ " , and the map key column type is "
						+ TableUtil.findColTypeWithAssertAndHint(modelSchema, mapKeyColNames[i])
					);
				}
			}
		}

		outputColNames = params.get(LookupParams.OUTPUT_COLS);
		if (null == outputColNames) {
			outputColNames = mapValueColNames;
		}
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		int m = this.mapValueColIndices.length;
		this.mapModel = new HashMap <>(modelRows.size());
		for (Row row : modelRows) {
			Object[] objects = new Object[m];
			for (int i = 0; i < m; i++) {
				objects[i] = row.getField(mapValueColIndices[i]);
			}

			List <Object> key = new ArrayList <>(mk);

			for (int i = 0; i < mk; i++) {
				key.add(row.getField(mapKeyColIndices[i]));
			}
			this.mapModel.put(key, objects);
		}
	}

	@Override
	public ModelMapper createNew(List <Row> newModelRows) {
		int m = this.mapValueColIndices.length;
		HashMap <List <Object>, Object[]> newMapModel = new HashMap <>(mapModel.size());

		switch (this.params.get(LookupParams.MODEL_STREAM_UPDATE_METHOD)) {
			case COMPLETE:
				break;
			case INCREMENT:
				for (List <Object> key : this.mapModel.keySet()) {
					newMapModel.put(key, this.mapModel.get(key));
				}
				break;
			default:
		}

		for (Row row : newModelRows) {
			Object[] objects = new Object[m];
			for (int i = 0; i < m; i++) {
				objects[i] = row.getField(mapValueColIndices[i]);
			}
			List <Object> key = new ArrayList <>(mk);

			for (int i = 0; i < mk; i++) {
				key.add(row.getField(mapKeyColIndices[i]));
			}
			newMapModel.put(key, objects);
		}
		this.mapModel = newMapModel;
		return this;
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema modelSchema, TableSchema dataSchema, Params params) {

		outputColNames = params.get(LookupParams.OUTPUT_COLS);
		String[] mapValueColNames = params.get(LookupParams.MAP_VALUE_COLS);
		String[] selectedColNames = params.get(LookupParams.SELECTED_COLS);

		if (null == outputColNames) {
			outputColNames = mapValueColNames;
		}
		TypeInformation <?>[] outputColTypes = (mapValueColNames == null)
			? TableUtil.findColTypesWithAssertAndHint(modelSchema, new String[] {modelSchema.getFieldNames()[1]})
			: TableUtil.findColTypesWithAssertAndHint(modelSchema, mapValueColNames);

		return Tuple4.of(selectedColNames, outputColNames, outputColTypes, params.get(LookupParams.RESERVED_COLS));
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		for (int i = 0; i < mk; i++) {
			this.currentKey.set(i, selection.get(i));
		}
		Object[] objects = this.mapModel.get(currentKey);
		if (null == objects) {
			objects = new Object[this.mapValueColIndices.length];
		}
		for (int i = 0; i < objects.length; ++i) {
			result.set(i, objects[i]);
		}
	}
}
