package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dataproc.KeyToValueParams;

import java.util.HashMap;
import java.util.List;

/**
 * Model Mapper for Key to Value operation.
 */
public class KeyToValueModelMapper extends ModelMapper {

	private static final long serialVersionUID = 2105766170811612439L;
	protected final int selectedColIdx;
	private final int mapKeyColIdx;
	private final int mapValueColIdx;
	private OutputColsHelper predResultColsHelper;

	private HashMap <Object, Object> kvmap;

	public KeyToValueModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		String mapKeyColName = params.get(KeyToValueParams.MAP_KEY_COL);
		this.mapKeyColIdx = TableUtil.findColIndexWithAssertAndHint(modelSchema, mapKeyColName);
		String mapValueColName = params.get(KeyToValueParams.MAP_VALUE_COL);
		this.mapValueColIdx = TableUtil.findColIndexWithAssertAndHint(modelSchema, mapValueColName);

		String selectedColName = params.get(KeyToValueParams.SELECTED_COL);
		this.selectedColIdx = TableUtil.findColIndexWithAssertAndHint(dataSchema, selectedColName);
		if (TableUtil.findColTypeWithAssertAndHint(dataSchema, selectedColName)
			!= TableUtil.findColTypeWithAssertAndHint(modelSchema, mapKeyColName)) {
			throw new IllegalArgumentException("Data types are not match. selected column type is "
				+ TableUtil.findColTypeWithAssertAndHint(dataSchema, selectedColName)
				+ " , and the map key column type is "
				+ TableUtil.findColTypeWithAssertAndHint(modelSchema, mapKeyColName)
			);
		}

		String outputColName = params.get(KeyToValueParams.OUTPUT_COL);
		if (null == outputColName) {
			outputColName = selectedColName;
		}

		this.predResultColsHelper = new OutputColsHelper(
			dataSchema,
			outputColName,
			TableUtil.findColTypeWithAssertAndHint(modelSchema, mapValueColName),
			params.get(KeyToValueParams.RESERVED_COLS)
		);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		try {
			this.kvmap = new HashMap <>();
			for (Row row : modelRows) {
				this.kvmap.put(row.getField(this.mapKeyColIdx), row.getField(this.mapValueColIdx));
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public TableSchema getOutputSchema() {
		return this.predResultColsHelper.getResultSchema();
	}

	@Override
	public Row map(Row row) throws Exception {
		return this.predResultColsHelper.getResultRow(row, Row.of(this.kvmap.get(row.getField(this.selectedColIdx))));
	}
}
