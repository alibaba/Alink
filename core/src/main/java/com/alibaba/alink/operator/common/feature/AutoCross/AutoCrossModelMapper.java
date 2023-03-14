package com.alibaba.alink.operator.common.feature.AutoCross;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.mapper.ComboModelMapper;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.params.feature.AutoCrossPredictParams;
import com.alibaba.alink.params.feature.featuregenerator.HasAppendOriginalData;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.pipeline.ModelExporterUtils;
import com.alibaba.alink.pipeline.PipelineStageBase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AutoCrossModelMapper extends ComboModelMapper {
	private static final long serialVersionUID = 4498117230717789425L;
	private String[] reversedCols;
	private String outputCol;
	private final AutoCrossPredictParams.OutputFormat outputFormat;
	boolean appendOriginalVec = true;

	//todo may support input vector col.
	public AutoCrossModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		outputFormat = params.get(AutoCrossPredictParams.OUTPUT_FORMAT);
	}

	@Override
	public List <Mapper> getLoadedMapperList() {
		List <Mapper> mapperList = new ArrayList <>();
		Collections.addAll(mapperList, this.mapperList.getMappers());
		return mapperList;
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		outputCol = params.get(AutoCrossPredictParams.OUTPUT_COL);
		String[] outputCols = new String[] {outputCol};
		TypeInformation[] outputTypes = new TypeInformation[] {AlinkTypes.VECTOR};
		reversedCols = params.get(HasReservedColsDefaultAsNull.RESERVED_COLS);
		return Tuple4.of(dataSchema.getFieldNames(), outputCols, outputTypes, reversedCols);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		List <Tuple3 <PipelineStageBase <?>, TableSchema, List <Row>>> stages =
			ModelExporterUtils.loadStagesFromPipelineModel(modelRows, getModelSchema());
		stages.get(1).f0.set(HasOutputCol.OUTPUT_COL, outputCol);
		stages.get(1).f0.set(AutoCrossPredictParams.OUTPUT_FORMAT, outputFormat);
		stages.get(1).f0.set(HasAppendOriginalData.APPEND_ORIGINAL_DATA, appendOriginalVec);
		if (reversedCols != null) {
			stages.get(1).f0.set(HasReservedColsDefaultAsNull.RESERVED_COLS, reversedCols);
		}

		this.mapperList = ModelExporterUtils
			.loadMapperListFromStages(stages, getDataSchema());
	}

}
