package com.confluent.checkride;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.io.FileInputStream;
import java.io.IOException;

public class Stream {
    final String USERDIR = System.getProperty("user.dir");
    private Topology topology; 
    private KafkaStreams streams;

    public Stream() throws IOException{
        streams = new KafkaStreams(getTopology(), getProperties());
    }
    private void start(){
        streams.start();
    }
    private Topology getTopology(){
        Topology dag;
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String,String> source;
        KStream<String,String> stockProcessor; 
        source = builder.stream("transactions");  
        stockProcessor = source.filter((key, value) -> (key.compareTo("CFLT") == 0));
        stockProcessor.to("cflt_transactions");
        dag = builder.build();
        return dag;
    }
    public Properties getProperties() throws IOException{
        Properties props = new Properties();
        props.load(new FileInputStream(USERDIR + "/configs/streamsConfig"));
        return props;
    }
    private void addShutdownHook(CountDownLatch latch){
        Runtime.getRuntime()
               .addShutdownHook(
                    new Thread(() ->
                    { 
                        streams.close();
                        latch.countDown();
                    })
               );
    }
    public static void main(String[] args) {
        CountDownLatch latch = new CountDownLatch(1);
        try{
            Stream stream = new Stream();
            stream.addShutdownHook(latch);
            stream.start();
            latch.await();
        }
        catch(Throwable e){
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}
