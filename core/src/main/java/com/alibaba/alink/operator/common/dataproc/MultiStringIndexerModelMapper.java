package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkIllegalOperationException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dataproc.HasHandleInvalid;
import com.alibaba.alink.params.dataproc.MultiStringIndexerPredictParams;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.pipeline.dataproc.MultiStringIndexerModel;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The model mapper for {@link MultiStringIndexerModel}.
 */
public class MultiStringIndexerModelMapper extends ModelMapper {

	private static final long serialVersionUID = 7434426152864663314L;
	/**
	 * A list maps that map token to index. Each map in the list corresponds to a column.
	 */
	private transient Map <Integer, Map <String, Long>> indexMapper;

	/**
	 * The default index for unseen tokens of each columns.
	 */
	private transient Map <Integer, Long> defaultIndex;

	private final String[] selectedColNames;
	private final HasHandleInvalid.HandleInvalid handleInvalidStrategy;

	public MultiStringIndexerModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);

		handleInvalidStrategy = this.params.get(MultiStringIndexerPredictParams.HANDLE_INVALID);
		selectedColNames = this.params.get(MultiStringIndexerPredictParams.SELECTED_COLS);
	}

	@Override
	public void loadModel(List <Row> modelRows) {
		MultiStringIndexerModelData model = new MultiStringIndexerModelDataConverter().load(modelRows);

		String[] trainColNames = model.meta.get(HasSelectedCols.SELECTED_COLS);
		int[] selectedColIndicesInModel = TableUtil.findColIndicesWithAssert(trainColNames, selectedColNames);
		this.indexMapper = new HashMap <>();
		this.defaultIndex = new HashMap <>();

		for (int i = 0; i < selectedColNames.length; i++) {
			Map <String, Long> mapper = new HashMap <>();
			int colIdxInModel = selectedColIndicesInModel[i];
			for (Tuple3 <Integer, String, Long> record : model.tokenAndIndex) {
				if (record.f0 == colIdxInModel) {
					String token = record.f1;
					Long index = record.f2;
					mapper.put(token, index);
				}
			}
			indexMapper.put(i, mapper);
			Long defaultIdx = model.tokenNumber.get(colIdxInModel);
			defaultIndex.put(i, defaultIdx);
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema modelSchema, TableSchema dataSchema, Params params) {

		String[] selectedColNames = params.get(MultiStringIndexerPredictParams.SELECTED_COLS);

		String[] outputColNames = params.get(MultiStringIndexerPredictParams.OUTPUT_COLS);
		if (outputColNames == null) {
			outputColNames = selectedColNames;
		}

		AkPreconditions.checkArgument(outputColNames.length == selectedColNames.length,
			new AkIllegalOperatorParameterException("OutputCol length must be equal to selectedCol length!"));

		String[] reservedColNames = params.get(MultiStringIndexerPredictParams.RESERVED_COLS);

		TypeInformation <?>[] outputColTypes = new TypeInformation[selectedColNames.length];
		Arrays.fill(outputColTypes, Types.LONG);

		return Tuple4.of(selectedColNames, outputColNames, outputColTypes, reservedColNames);
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		for (int i = 0; i < selectedColNames.length; i++) {
			Object val = selection.get(i);
			String key = val == null ? null : String.valueOf(val);
			Long index = indexMapper.get(i).get(key);
			if (index != null) {
				result.set(i, index);
			} else {
				switch (this.handleInvalidStrategy) {
					case KEEP:
						result.set(i, defaultIndex.get(i));
						break;
					case SKIP:
						result.set(i, null);
						break;
					case ERROR:
						throw new AkIllegalDataException("Unseen token: " + key);
					default:
						throw new AkUnsupportedOperationException("Invalid handle invalid strategy.");
				}
			}
		}
	}
}
