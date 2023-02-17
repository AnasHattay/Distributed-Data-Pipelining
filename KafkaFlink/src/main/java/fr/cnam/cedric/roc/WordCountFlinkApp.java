package fr.cnam.cedric.roc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class WordCountFlinkApp{

    public static void main(String[] args) throws  Exception{

        String inputTopic="topic1";
        String outputTopic="topic2";
        String server="localhost:9092";
        String groupId="fr.cnam.credic.roc";
        
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        
        KafkaSource<String> source = createKafkaSource(inputTopic,server,groupId);
        //source.print(); // Add this line to log the data received from topic 1
       // System.out.print(source);

        DataStream<String> text = environment.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        
        //DataStream<String> countOperator = countOperation(text);
        
	DataStream<Tuple3<String, Double, Double>> tupleStream = aggregateData(text);
	//DataStream<String> outputDataStream = DataStream.map(tuple -> tuple.f0 + "," + tuple.f1.tostring() + "," + tuple.f2.tostring());
	DataStream<String> stringStream = tupleStream.map(tuple -> {
    	   return tuple.f0 + "," + tuple.f1.toString() + "," + tuple.f2.toString();
//+ "," + tuple.f3.toString() + "," + tuple.f4.toString() + "," + tuple.f5.toString() + "," + tuple.f6.toString() + "," + tuple.f7.toString() + "," + tuple.f8.toString() + "," + tuple.f9.toString();
       }); 
 KafkaSink<String> sink =  createKafkaSink(outputTopic, server);
        
        stringStream.sinkTo(sink);
        
        environment.execute("WordCountFlinkApp");
    }

   

    public static KafkaSource<String> createKafkaSource(String inputTopic, String server, String groupId) throws Exception{

        
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
       .setBootstrapServers(server)
       .setTopics(inputTopic)
       .setGroupId(groupId)
       .setStartingOffsets(OffsetsInitializer.earliest())
       .setValueOnlyDeserializer(new SimpleStringSchema())
       .build();
        return kafkaSource;
    }
    
    
    public static DataStream<String> countOperation(DataStream<String> text){
    	
    	
    	 DataStream<String> counts = text.flatMap(new Tokenizer())
    				// Group by the tuple field "0" and sum up tuple field "1"
    				.keyBy(value -> value.f0)
    				.sum(1)
    				.flatMap(new Reducer());
    	return counts;
    }
    public static DataStream<Tuple3<String, Double, Double>> aggregateData(DataStream<String> text){ 
     // filter the data stream to include only the data you are interested in
	 DataStream<String> filteredStream = text.filter(x -> x.contains("cpu_percent") || x.contains("memory_percent") );
//|| x.contains("/dev/vda5_used") || x.contains("swap_percent") || x.contains("bytes_sent") ||  x.contains("/dev/vda5_total") || x.contains("bytes_recv") || x.contains("/dev/vda5_free") || x.contains("/dev/vda5_percent") );

 	 // transform the data stream into the desired format
   	 DataStream<Tuple3<String, Double, Double>> transformedStream = filteredStream.map(x -> {
                 String[] parts = x.split(",");
                 String vmId = parts[0];
                 String metricType = parts[1];
                 Double metricValue = Double.parseDouble(parts[2]);
                 return new Tuple3<>(vmId, metricType.equals("cpu_percent") ? metricValue : null, metricType.equals("memory_percent") ? metricValue : null );
//metricType.equals("/dev/vda5_used") ? metricValue : null , metricType.equals("bytes_recv") ? metricValue : null, metricType.equals("swap_percent") ? metricValue : null ,metricType.equals("bytes_sent") ? metricValue : null , metricType.equals("/dev/vda5_total") ? metricValue : null , metricType.equals("/dev/vda5_free") ? metricValue : null , metricType.equals("/dev/vda5_percent") ? metricValue : null);
             })
             .returns(TypeInformation.of(new TypeHint<Tuple3<String, Double, Double>>(){}));
          //   String[] parts = x.split(",");
       	    // String vmId = parts[0];
            // String metricType = parts[1];
            // Double metricValue = Double.parseDouble(parts[2]);
            // return new Tuple3<String, Double,Double>(vmId, metricType.equals("cpu_percent") ? metricValue : null, metricType.equals("memory_percent") ? metricValue : null );
// metricType.equals("/dev/vda5_used") ? metricValue : null, metricType.equals("bytes_recv") ? metricValue : null, metricType.equals("swap_percent") ? metricValue : null, metricType.equals("bytes_sent") ? metricValue : null, metricType.equals("/dev/vda5_total") ? metricValue : null, metricType.equals("/dev/vda5_free") ? metricValue : null, metricType.equals("/dev/vda5_percent") ? metricValue : null);

//             return new Tuple3<>(vmId, metricType.equals("cpu_percent") ? metricValue : null, metricType.equals("memory_percent") ? metricValue : null, metricType.equals("/dev/vda5_used") ? metricValue : null , metricType.equals("bytes_recv") ? metricValue : null, metricType.equals("swap_percent") ? metricValue : null ,metricType.equals("bytes_sent") ? metricValue : null , metricType.equals("/dev/vda5_total") ? metricValue : null , metricType.equals("/dev/vda5_free") ? metricValue : null , metricType.equals("/dev/vda5_percent") ? metricValue : null);
         

         // group the data stream by the key (VM ID)
//         KeyedStream<Tuple3<String, Double, Double>, String> keyedStream = transformedStream.keyBy(x -> x.f0);



         return transformedStream;  
    } 
    
    public static KafkaSink<String> createKafkaSink(String outputTopic, String server) {
    	
    	KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder()
    			.setValueSerializationSchema(new SimpleStringSchema())
    			.setTopic(outputTopic)
    			.build();
    	
    	
        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers(server)
            .setRecordSerializer(serializer)
            .build();

        return sink;
    }



 // public static KafkaSink<String> createKafkaSinkp(String outputTopic, String server) {

   //     KafkaRecordSerializationSchema<Tuple3<String, Double, Double>> serializer =
     //       KafkaRecordSerializationSchema.<Tuple3<String, Double, Double>>builder()
       //             .setTopic(outputTopic)
         //           .setValueSerializer(new TypeInformationSerializationSchema<>(Types.TUPLE(Types.STRING, Types.DOUBLE, Types.DOUBLE)))
           //         .build();
      //  KafkaSink<String> sink = KafkaSink.<String>builder()
        //    .setBootstrapServers(server)
           //.setRecordSerializer(serializer)
           // .build();

    //    return sink;
   // }

}
