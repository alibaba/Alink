package com.alibaba.alink.operator.stream.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.common.io.kafka.Kafka011SourceBuilder;
import com.alibaba.alink.params.io.Kafka011SourceParams;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * Data source for kafka.
 */
@IoOpAnnotation(name = "kafka011", hasTimestamp = true, ioType = IOType.SourceStream)
public final class Kafka011SourceStreamOp extends BaseSourceStreamOp<Kafka011SourceStreamOp>
        implements Kafka011SourceParams<Kafka011SourceStreamOp> {

    final static String[] colNames = new String[]{"message_key", "message", "topic", "topic_partition", "partition_offset"};
    final static TypeInformation[] colTypes = new TypeInformation[]{Types.STRING,
        Types.STRING, Types.STRING, Types.INT, Types.LONG};

    public Kafka011SourceStreamOp() {
        this(new Params());
    }

    public Kafka011SourceStreamOp(Params params) {
        super(AnnotationUtils.annotatedName(Kafka011SourceStreamOp.class), params);
    }

    /**
     * Parse a string to unix time stamp in miliseconds.
     */
    public static long parseDateStringToMs(String dateStr, String dataFormat) {
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dataFormat);
            return simpleDateFormat.parse(dateStr).getTime();
        } catch (Exception e) {
            throw new RuntimeException("Fail to getVector date string: " + dateStr);
        }
    }

    @Override
    protected Table initializeDataSource() {
        String topic = getTopic();
        String startupMode = getStartupMode();

        Properties props = new Properties();
        props.setProperty("group.id", getGroupId());
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());

        Kafka011SourceBuilder builder = new Kafka011SourceBuilder();

        String topicPattern = getTopicPattern();
        if (!StringUtils.isNullOrWhitespaceOnly(topicPattern)) {
            builder.setTopicPattern(topicPattern);
        } else {
            List<String> topics = new ArrayList<String>();
            if (!StringUtils.isNullOrWhitespaceOnly(topic)) {
                topics.add(topic);
            } else {
                throw new IllegalArgumentException("either topic or topicPattern should be set.");
            }
            builder.setTopic(topics);
        }
        builder.setProperties(props);
        builder.setBaseRowTypeInfo(new RowTypeInfo(colTypes, colNames));
        builder.setStartupMode(startupMode);

        if (startupMode.equalsIgnoreCase("TIMESTAMP")) {
            String formatString = "yyyy-MM-dd HH:mm:ss";
            builder.setStartTimeMs(parseDateStringToMs(getStartTime(), formatString));
        }

        DataStream<Row> data = MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamExecutionEnvironment()
                .addSource(builder.build()).name("kafka011");
        return DataStreamConversionUtil.toTable(getMLEnvironmentId(), data, colNames, colTypes);
    }
}

