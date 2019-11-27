package com.alibaba.alink.operator.common.nlp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.model.ModelDataConverter;
import com.alibaba.alink.common.VectorTypes;

import java.util.List;

public class Word2VecModelDataConverter implements ModelDataConverter<Word2VecModelDataConverter, Word2VecModelDataConverter> {
	public List <Row> modelRows;

	@Override
	public void save(Word2VecModelDataConverter modelData, Collector<Row> collector) {
		modelData.modelRows.forEach(collector::collect);
	}

	@Override
	public Word2VecModelDataConverter load(List <Row> rows) {
		modelRows = rows;
		return this;
	}

	@Override
	public TableSchema getModelSchema() {
		return new TableSchema(
			new String[] {"word", "vec"},
			new TypeInformation[] {Types.STRING, VectorTypes.VECTOR}
		);
	}
}
