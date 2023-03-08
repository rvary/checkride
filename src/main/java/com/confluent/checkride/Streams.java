package com.confluent.checkride;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsBuilder;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;

public class Streams {
    public static void main(String[] args) throws Exception {
        Properties streamProperties = new Properties();
        try{
            FileInputStream fis = new FileInputStream("/Users/ryanvary/Development/streams/checkride-app/src/configs/streamsConfig");
            streamProperties.load(fis);
            System.out.println("Hello, Maven");
        } catch(IOException io){
            io.printStackTrace();
        }
        StreamsBuilder topology = new StreamsBuilder();
    }
}
