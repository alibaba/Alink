package com.alibaba.alink.operator.common.tensorflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.dl.utils.ZipFileUtil;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.params.tensorflow.savedmodel.TFTableModelPredictParams;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Comparator;
import java.util.List;

/**
 * {@link TFTableModelPredictModelMapper} provides inference for SavedModel which is in Alink Model format.
 */
public class TFTableModelPredictModelMapper extends ModelMapper implements Serializable {

	private final BaseTFSavedModelPredictRowMapper mapper;

	public TFTableModelPredictModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		mapper = new BaseTFSavedModelPredictRowMapper(dataSchema, params);
	}

	@Override
	public void open() {
		mapper.open();
	}

	@Override
	public void close() {
		mapper.close();
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		String workDir;
		try {
			workDir = PythonFileUtils.createTempWorkDir("temp_");
		} catch (Exception e) {
			throw new RuntimeException("Cannot create temporary work directory.", e);
		}

		modelRows = new ArrayList <>(modelRows);
		modelRows.sort(Comparator.comparingLong(d -> (Long) d.getField(0)));

		String zipFilename = (String) modelRows.get(0).getField(1);
		Path zipPath = Paths.get(workDir, zipFilename);
		try (FileOutputStream fos = new FileOutputStream(zipPath.toFile())) {
			Decoder decoder = Base64.getDecoder();
			for (int i = 1, modelRowsSize = modelRows.size(); i < modelRowsSize; i += 1) {
				Row modelRow = modelRows.get(i);
				fos.write(decoder.decode((String) modelRow.getField(1)));
			}
		} catch (Exception e) {
			throw new RuntimeException(String.format("Cannot extract data to %s", zipFilename), e);
		}

		String targetDir = zipFilename.substring(0, zipFilename.indexOf(".zip"));
		Path targetPath = Paths.get(workDir, targetDir);
		try {
			ZipFileUtil.unzipFileIntoDirectory(zipPath.toFile(), targetPath.toFile());
		} catch (IOException e) {
			throw new RuntimeException(String.format("Failed to unzip %s to %s.", zipPath.toString(), targetPath.toString()), e);
		}
		mapper.setModelPath(targetPath.toAbsolutePath().toString());
	}

	@Override
	public ModelMapper createNew(List <Row> newModelRows) {
		return super.createNew(newModelRows);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		String[] tfInputCols = params.get(TFTableModelPredictParams.SELECTED_COLS);
		if (null == tfInputCols) {
			tfInputCols = dataSchema.getFieldNames();
		}
		String tfOutputSchemaStr = params.get(TFTableModelPredictParams.OUTPUT_SCHEMA_STR);
		TableSchema tfOutputSchema = CsvUtil.schemaStr2Schema(tfOutputSchemaStr);
		String[] reservedCols = params.get(TFTableModelPredictParams.RESERVED_COLS);

		return Tuple4.of(tfInputCols,
			tfOutputSchema.getFieldNames(),
			tfOutputSchema.getFieldTypes(),
			reservedCols);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		mapper.map(selection, result);
	}
}
