package samples.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.*;
import java.io.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class SimpleProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");         
        props.put("value.serializer", "samples.kafka.JsonSerializer");
                    
        Producer<String, JSONObject> producer = new KafkaProducer <>(props);
        String topic = "movies-sample";

        SimpleProducer sp = new SimpleProducer();
        String path = "/home/ragesh/eclipseworkspace/kafka/resources/movies.json";
        JSONParser jsonParser = new JSONParser();
        
        try {
            FileReader fReader = new FileReader(path);
            Object obj = jsonParser.parse(fReader);
            JSONArray moviesList = (JSONArray) obj;
            //System.out.println(moviesList);
            moviesList.forEach( movie -> sp.sendToKafkaTopic(producer, topic, "movies",(JSONObject)movie ));
            
            fReader.close();
            producer.close();
        }
        catch(Exception ex) {
            System.out.println("Exception while reading the file: " + ex.getMessage());
        }
    }
    
    public void sendToKafkaTopic(Producer<String, JSONObject> kafkaProducer, String topic, String key, JSONObject value) {
        try {
            ProducerRecord<String, JSONObject> record = new ProducerRecord<>(topic,key,value);
            kafkaProducer.send(record);
            Thread.sleep(1000);
        }
        catch(Exception ex) {
            System.out.println("Exception while sending data: " + ex.getMessage());
        }
    }
}

