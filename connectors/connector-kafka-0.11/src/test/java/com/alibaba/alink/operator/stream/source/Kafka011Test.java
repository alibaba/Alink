package com.alibaba.alink.operator.stream.source;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.Kafka011SinkStreamOp;
import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class Kafka011Test {
    /**
     * We have a single embedded Kafka server that gets started when this test class is initialized.
     * <p>
     * It's automatically started before any methods are run via the @ClassRule annotation.
     * It's automatically stopped after all of the tests are completed via the @ClassRule annotation.
     */
    @ClassRule
    public static final SharedKafkaTestResource SHARED_KAFKA_TEST_RESOURCE = new SharedKafkaTestResource();

    @Test
    public void testKafka011Sink() throws Exception {
        Row[] rows = new Row[]{
            Row.of(1L, 1L, 1.0),
            Row.of(2L, 2L, 1.0),
            Row.of(2L, 3L, 1.0),
        };

        final String topicName = "topic_1";
        KafkaTestUtils kafkaTestUtils = SHARED_KAFKA_TEST_RESOURCE.getKafkaTestUtils();
        kafkaTestUtils.createTopic(topicName, 1, (short) 1);

        StreamOperator data = new MemSourceStreamOp(rows, new String[]{"f1", "f2", "f3"});

        StreamOperator sink = new Kafka011SinkStreamOp()
            .setBootstrapServers(SHARED_KAFKA_TEST_RESOURCE.getKafkaConnectString())
            .setDataFormat("csv")
            .setTopic(topicName);

        data.link(sink);
        StreamOperator.execute();

        int s = kafkaTestUtils.consumeAllRecordsFromTopic(topicName).size();
        Assert.assertEquals(s, 3);
    }

    @Test
    public void testKafka011Source() throws Exception {
        final String topicName = "topic_2";
        KafkaTestUtils kafkaTestUtils = SHARED_KAFKA_TEST_RESOURCE.getKafkaTestUtils();
        kafkaTestUtils.createTopic(topicName, 1, (short) 1);
        kafkaTestUtils.produceRecords(4, topicName, 0);

        StreamOperator data = new Kafka011SourceStreamOp()
            .setBootstrapServers(SHARED_KAFKA_TEST_RESOURCE.getKafkaConnectString())
            .setGroupId("g")
            .setStartupMode("earliest")
            .setTopic(topicName);

        Assert.assertEquals(data.getColNames().length, 5);
    }
}