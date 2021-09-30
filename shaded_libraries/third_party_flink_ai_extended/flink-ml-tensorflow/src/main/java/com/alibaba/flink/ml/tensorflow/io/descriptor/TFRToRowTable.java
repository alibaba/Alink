package com.alibaba.flink.ml.tensorflow.io.descriptor;

import com.alibaba.flink.ml.tensorflow.io.TFRExtractRowHelper;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.alibaba.flink.ml.tensorflow.io.descriptor.TFRToRowTableValidator.*;

public class TFRToRowTable extends ConnectorDescriptor {
    DescriptorProperties properties = new DescriptorProperties();

    public TFRToRowTable() {
        super("TFRToRowTable", 1, false);
    }

    public TFRToRowTable paths(String[] paths) {
        List<List<String>> pathsList = new ArrayList<>();
        for (String path : paths) {
            pathsList.add(Collections.singletonList(path));
        }
        properties.putIndexedFixedProperties(CONNECTOR_PATH, Collections.singletonList("name"), pathsList);
        return this;
    }

    public TFRToRowTable epochs(int epochs) {
        properties.putInt(CONNECTOR_EPOCHS, epochs);
        return this;
    }

    public TFRToRowTable outColAliases(String[] outColAliases) {
        List<List<String>> outColAliasesList = new ArrayList<>();
        for (String alias : outColAliases) {
            outColAliasesList.add(Collections.singletonList(alias));
        }
        properties
                .putIndexedFixedProperties(CONNECTOR_OUT_COL_ALIASES, Collections.singletonList("name"),
                        outColAliasesList);
        return this;
    }

    public TFRToRowTable converters(TFRExtractRowHelper.ScalarConverter[] converters) {
        List<List<String>> res = new ArrayList<>();
        for (TFRExtractRowHelper.ScalarConverter converter : converters) {
            res.add(Collections.singletonList(converter.name()));
        }
        properties.putIndexedFixedProperties(CONNECTOR_CONVERTERS, Collections.singletonList("name"), res);
        return this;
    }


    @Override
    protected Map<String, String> toConnectorProperties() {
        return properties.asMap();
    }
}
