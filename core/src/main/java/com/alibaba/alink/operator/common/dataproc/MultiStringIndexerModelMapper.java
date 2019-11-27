package com.alibaba.alink.operator.common.dataproc;


import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dataproc.MultiStringIndexerPredictParams;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.pipeline.dataproc.MultiStringIndexerModel;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The model mapper for {@link MultiStringIndexerModel}.
 */
public class MultiStringIndexerModelMapper extends ModelMapper {

    /**
     * A list maps that map token to index. Each map in the list corresponds to a column.
     */
    private transient Map<Integer, Map<String, Long>> indexMapper;

    /**
     * The default index for unseen tokens of each columns.
     */
    private transient Map<Integer, Long> defaultIndex;

    private OutputColsHelper outputColsHelper;
    private String[] selectedColNames;
    private int[] selectedColIndicesInData;
    private StringIndexerUtil.HandleInvalidStrategy handleInvalidStrategy;

    public MultiStringIndexerModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params);
        selectedColNames = params.get(MultiStringIndexerPredictParams.SELECTED_COLS);
        String[] outputColNames = params.get(MultiStringIndexerPredictParams.OUTPUT_COLS);
        if (outputColNames == null) {
            outputColNames = selectedColNames;
        }
        String[] reservedColNames = params.get(MultiStringIndexerPredictParams.RESERVED_COLS);

        handleInvalidStrategy = StringIndexerUtil.HandleInvalidStrategy
            .valueOf(params.get(MultiStringIndexerPredictParams.HANDLE_INVALID).toUpperCase());

        TypeInformation[] outputColTypes = new TypeInformation[selectedColNames.length];
        Arrays.fill(outputColTypes, Types.LONG);

        outputColsHelper = new OutputColsHelper(dataSchema, outputColNames, outputColTypes, reservedColNames);
    }

    @Override
    public void loadModel(List<Row> modelRows) {
        MultiStringIndexerModelData model = new MultiStringIndexerModelDataConverter().load(modelRows);

        String[] trainColNames = model.meta.get(HasSelectedCols.SELECTED_COLS);
        this.selectedColIndicesInData = TableUtil.findColIndices(super.getDataSchema(), selectedColNames);
        int[] selectedColIndicesInModel = TableUtil.findColIndices(trainColNames, selectedColNames);
        this.indexMapper = new HashMap<>();
        this.defaultIndex = new HashMap<>();

        for (int i = 0; i < selectedColNames.length; i++) {
            Map<String, Long> mapper = new HashMap<>();
            int colIdxInModel = selectedColIndicesInModel[i];
            for (Tuple3<Integer, String, Long> record : model.tokenAndIndex) {
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
    public TableSchema getOutputSchema() {
        return outputColsHelper.getResultSchema();
    }

    @Override
    public Row map(Row row) throws Exception {
        Row result = new Row(selectedColNames.length);
        for (int i = 0; i < selectedColNames.length; i++) {
            Map<String, Long> mapper = indexMapper.get(i);
            int colIdxInData = selectedColIndicesInData[i];
            Object val = row.getField(colIdxInData);
            String key = val == null ? null : String.valueOf(val);
            Long index = mapper.get(key);
            if (index != null) {
                result.setField(i, index);
            } else {
                switch (this.handleInvalidStrategy) {
                    case KEEP:
                        Long localDefaultIndex = defaultIndex.get(i);
                        result.setField(i, localDefaultIndex);
                        break;
                    case SKIP:
                        result.setField(i, null);
                        break;
                    case ERROR:
                        throw new RuntimeException("Unseen token: " + key);
                    default:
                        throw new IllegalArgumentException("Invalid handle invalid strategy.");
                }
            }
        }
        return outputColsHelper.getResultRow(row, result);
    }
}
