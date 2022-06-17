package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dataproc.LookupRecentDaysParams;
import org.apache.commons.lang.SerializationUtils;

import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.HashMap;
import java.util.List;

public class LookupRecentDaysModelMapper extends ModelMapper {

	private int numGroups;
	private HashMap <List <Object>, Object[]>[] maps;
	private int[][] groupColIndexes;
	private List <Object>[] keyObjs;
	private int[] valueLens;

	String[] inputCols;

	//private final String featureSchemaStr;

	public LookupRecentDaysModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);

		this.inputCols = dataSchema.getFieldNames();

		//this.featureSchemaStr = params.get(LookupRecentDaysParams.FEATURE_SCHEMA_STR);

	}

	@Override
	public void loadModel(List <Row> modelRows) {
		for (Row row : modelRows) {
			if (0 == (Integer) row.getField(0)) {
				Params modelParams = Params.fromJson((String) row.getField(1));
				//params.get("featureSchemaStr", String .class);
				numGroups = modelParams.get("numGroups", Integer.class);
				String[][] groupColsArray = modelParams.get("groupColsArray", String[][].class);
				this.valueLens = modelParams.get("valueLens", int[].class);
				this.groupColIndexes = new int[numGroups][];
				this.keyObjs = new List[numGroups];
				for (int i = 0; i < numGroups; i++) {
					groupColIndexes[i] = TableUtil.findColIndices(inputCols, groupColsArray[i]);
					keyObjs[i] = Arrays.asList(new Object[groupColIndexes[i].length]);
				}
				this.maps = new HashMap[numGroups];
				for (int i = 0; i < numGroups; i++) {
					maps[i] = new HashMap <>();
				}
				break;
			}
		}
		Decoder base64Decoder = Base64.getDecoder();
		for (Row row : modelRows) {
			Integer k = (Integer) row.getField(0);
			if (k > 0) {
				k -= 1;
				maps[k].put(
					Arrays.asList(
						(Object[]) SerializationUtils.deserialize(base64Decoder.decode((String) row.getField(1)))),
					(Object[]) SerializationUtils.deserialize(base64Decoder.decode((String) row.getField(2)))
				);
			}
		}
	}

	@Override
	public ModelMapper createNew(List <Row> newModelRows) {
		//todo
		return this;
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema modelSchema, TableSchema dataSchema, Params params) {

		TableSchema schema = TableUtil.schemaStr2Schema(params.get(LookupRecentDaysParams.FEATURE_SCHEMA_STR));

		return Tuple4.of(dataSchema.getFieldNames(), schema.getFieldNames(), schema.getFieldTypes(),
			params.get(LookupRecentDaysParams.RESERVED_COLS));
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		int k = 0;
		for (int i = 0; i < numGroups; i++) {
			for (int j = 0; j < groupColIndexes[i].length; j++) {
				if (groupColIndexes[i][j] == -1) {
					keyObjs[i].set(j, 1);
				} else {
					keyObjs[i].set(j, selection.get(groupColIndexes[i][j]));
				}
			}

			Object[] valueObjs = maps[i].get(keyObjs[i]);
			if (null == valueObjs) {
				for (int j = 0; j < valueLens[i]; j++) {
					result.set(k, null);
					k++;
				}
			} else {
				for (Object obj : valueObjs) {
					result.set(k, obj);
					k++;
				}
			}
		}
	}
}