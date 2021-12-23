package com.alibaba.flink.ml.tensorflow2.io;

import com.alibaba.flink.ml.operator.util.TypeUtil;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.alibaba.flink.ml.tensorflow2.io.descriptor.TFRToRowTableValidator.*;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

public class TFRToRowTableSourceFactory implements TableSourceFactory<Row> {
    @Override
    public TableSource<Row> createTableSource(Context context) {
        DescriptorProperties properties = new DescriptorProperties();
        properties.putProperties(context.getTable().toProperties());

        String[] paths = getPaths(properties);
        int epochs = getEpochs(properties);
        String[] outColAliases = getOutColAliases(properties);
        TFRExtractRowHelper.ScalarConverter[] converters = getConverters(properties);
        RowTypeInfo outRowType = TypeUtil.schemaToRowTypeInfo(context.getTable().getSchema());
        if (outColAliases != null) {

            return new TFRToRowTableSource(paths, epochs, outRowType,
                    outColAliases, converters);
        } else {
            return new TFRToRowTableSource(paths, epochs, outRowType, converters);
        }

    }

    private TFRExtractRowHelper.ScalarConverter[] getConverters(DescriptorProperties properties) {
        return properties.getFixedIndexedProperties(CONNECTOR_CONVERTERS, Collections.singletonList("name"))
                .stream().map(m -> m.get("name"))
                .map(k -> TFRExtractRowHelper.ScalarConverter.valueOf(properties.getString(k)))
                .toArray(TFRExtractRowHelper.ScalarConverter[]::new);
    }

    private String[] getOutColAliases(DescriptorProperties properties) {
        if (!properties.containsKey(CONNECTOR_OUT_COL_ALIASES)) {
            return null;
        }
        return properties.getFixedIndexedProperties(CONNECTOR_OUT_COL_ALIASES, Collections.singletonList("name"))
                .stream().map(m -> m.get("name")).map(properties::getString).toArray(String[]::new);
    }

    private int getEpochs(DescriptorProperties properties) {
        return properties.getInt(CONNECTOR_EPOCHS);
    }

    private String[] getPaths(DescriptorProperties properties) {
        return properties.getFixedIndexedProperties(CONNECTOR_PATH, Collections.singletonList("name"))
                .stream().map(m -> m.get("name")).map(properties::getString).toArray(String[]::new);
    }

    @Override
    public Map<String, String> requiredContext() {
        return Collections.singletonMap(CONNECTOR_TYPE, "TFRToRowTable");
    }

    @Override
    public List<String> supportedProperties() {
        return Collections.singletonList("*");
    }
}
