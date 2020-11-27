package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.SISOModelMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexToStringModelMapper extends SISOModelMapper {

	private static final long serialVersionUID = 6871044865482642336L;
	private Map <Long, String> mapper;

	public IndexToStringModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
	}

	@Override
	protected TypeInformation initPredResultColType() {
		return Types.STRING;
	}

	@Override
	protected Object predictResult(Object input) throws Exception {
		return mapper.get((Long) input);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		List <Tuple2 <String, Long>> model = new StringIndexerModelDataConverter().load(modelRows);
		this.mapper = new HashMap <>();
		for (Tuple2 <String, Long> record : model) {
			this.mapper.put(record.f1, record.f0);
		}
	}
}
