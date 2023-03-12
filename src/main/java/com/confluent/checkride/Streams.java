package com.confluent.checkride;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;

public class Streams {
    public static void main(String[] args) throws Exception {
        Properties streamProperties = new Properties();
        try{
            String config = System.getProperty("user.dir") + "/configs/streamsConfig";
            FileInputStream fis = new FileInputStream(config);
            streamProperties.load(fis);
            System.out.println("Hello, Maven");
        } catch(IOException io){
            io.printStackTrace();
        }
       StreamsBuilder topology = new StreamsBuilder();
       KStream<String, Integer> source = topology.stream("transactions");    
    }
}
