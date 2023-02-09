package fr.cnam.cedric.roc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.api.datastream.DataStream;
import java.util.Properties;

public class Flink_Kafka_Receiver {

    public static void main(String[] args) throws  Exception{

        String inputTopic="testtopic";
        String server="localhost:9092";
        StramConsumrer(inputTopic,server);
    }

    public static void StramConsumrer(String inputTopic, String server) throws Exception{

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer011<String> flinkKafkaConsumer = createStringConsumerForTopic(inputTopic, server);
        DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);
        stringInputStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "Receiving from Kafka : " + value;
            }
        }).print();
        environment.execute();
    }

    private static FlinkKafkaConsumer011<String> createStringConsumerForTopic(String inputTopic, String server) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        //props.setProperty("group.id",kafkaGroup);
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(inputTopic, new SimpleStringSchema(), properties);
        return consumer;
    }
}
