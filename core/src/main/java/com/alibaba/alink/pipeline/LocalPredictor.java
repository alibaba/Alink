package com.alibaba.alink.pipeline;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.mapper.MapperChain;
import com.alibaba.alink.common.utils.TableUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A LocalPredictor which is generated from {@link LocalPredictable} predict an instance to one or more instances using
 * map or flatMap accordingly.
 * <p>
 * The most important feature of LocalPredictor is that it can run at local and thus we can deploy the predictor to
 * another system.
 */
public class LocalPredictor {
	protected final ArrayList <Mapper> mappers = new ArrayList <>();
	protected MapperChain mapperList;

	public LocalPredictor(String pipelineModelPath, String inputSchemaStr) throws Exception {
		this(new FilePath(pipelineModelPath), TableUtil.schemaStr2Schema(inputSchemaStr));
	}

	public LocalPredictor(FilePath pipelineModelPath, String inputSchemaStr) throws Exception {
		this(pipelineModelPath, TableUtil.schemaStr2Schema(inputSchemaStr));
	}

	public LocalPredictor(FilePath pipelineModelPath, TableSchema inputSchema) throws Exception {
		this(
			Preconditions.checkNotNull(
				ModelExporterUtils
					.loadLocalPredictorFromPipelineModelAsMappers(
						pipelineModelPath, inputSchema
					),
				"The input mappers can not be empty."
			)
		);
	}

	public LocalPredictor(List <Row> pipelineModel, TableSchema modelSchema, TableSchema inputSchema)
		throws Exception {
		this(
			Preconditions.checkNotNull(
				ModelExporterUtils
					.loadLocalPredictorFromPipelineModel(
						pipelineModel, modelSchema, inputSchema
					),
				"The input mappers can not be empty."
			).mappers.toArray(new Mapper[0])
		);
	}

	public LocalPredictor(Mapper... mappers) {
		if (null == mappers || 0 == mappers.length) {
			throw new RuntimeException("The input mappers can not be empty.");
		}

		this.mappers.addAll(Arrays.asList(mappers));

		this.mapperList = new MapperChain(this.mappers.toArray(new Mapper[0]));

		this.mapperList.open();
	}

	public void merge(LocalPredictor otherPredictor) {
		this.mappers.addAll(otherPredictor.mappers);
		this.mapperList = new MapperChain(this.mappers.toArray(new Mapper[0]));
	}

	public TableSchema getOutputSchema() {
		if (mappers.size() > 0) {
			return mappers.get(mappers.size() - 1).getOutputSchema();
		} else {
			return null;
		}
	}

	/**
	 * map operation method that maps a row to a new row.
	 *
	 * @param row the input Row type data
	 * @return one Row type data
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation to fail.
	 */
	public Row map(Row row) throws Exception {
		return this.mapperList.map(row);
	}

	/**
	 * predict operation
	 *
	 * @param inputs support single object and object array, and also support a Row type input.
	 * @return prediction with the format of Object Array
	 * @throws Exception
	 */
	public Object[] predict(Object... inputs) throws Exception {
		Row row = map((1 == inputs.length && (inputs[0] instanceof Row)) ? (Row) inputs[0] : Row.of(inputs));
		Object[] objs = new Object[row.getArity()];
		for (int i = 0; i < objs.length; i++) {
			objs[i] = row.getField(i);
		}
		return objs;
	}

	@Deprecated
	public void open() {
	}

	public void close() {
		this.mapperList.close();
		//this.mappers.forEach(Mapper::close);
	}

}
