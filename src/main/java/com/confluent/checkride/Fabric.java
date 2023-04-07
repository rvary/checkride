package com.confluent.checkride;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;

import org.json.JSONObject;

public class Fabric {
    private KafkaStreams stream;

    public Fabric() throws IOException{
        Properties props = new Properties();
        props.load(new FileInputStream(System.getProperty("user.dir") + "/configs/brokerageStream"));
        stream = new KafkaStreams(getTopology(), props);
        stream.cleanUp();
    }
    private Topology getTopology(){
        Topology dag;
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String,String> processed, validated, invalidated, stockVolume;
        KStream<String,Long> invalidatedCount;
        KTable<String,String> brokerageAccounts;
        KTable<String,Long> validatedCount;

        processed = builder.stream("processed_transaction");

        brokerageAccounts = processed.toTable();
        brokerageAccounts
            .toStream()
            .to("brokerage_accounts"); 
            
        validated = builder.stream("validated_transaction_request");
        
        stockVolume = validated
            .map((holder, record) -> {
                JSONObject transaction = new JSONObject(record);
                JSONObject mapped = new JSONObject();
                String stock;

                stock = transaction.getString("symbol");
                mapped.put("shares",transaction.getInt("shares"));
                mapped.put("price",transaction.getDouble("price"));
                mapped.put("transactionValue", transaction.getDouble("transactionValue"));
                mapped.put("transactionType", transaction.getString("transactionType"));

                return new KeyValue<String,String>(stock, mapped.toString()); 
            });

        stockVolume
            .groupByKey()
            .aggregate(new Initializer<String>(){
                @Override
                public String apply(){
                    JSONObject stockVolume = new JSONObject();
                    stockVolume.put("stock", "");
                    stockVolume.put("shareVolume", 0);
                    stockVolume.put("value", 0.0);
                    stockVolume.put("purchases", 0);
                    stockVolume.put("avgPrice", 0.0);
                    stockVolume.put("sales", 0);
                    return stockVolume.toString();
                }
            }, new Aggregator<String, String, String>() {
                JSONObject a, agg;
                Double transactionValue, aggregatePrice, avg;
                int transactionShares, volume; 
                @Override
                public String apply(String stock, String transaction, String aggregate){
                    a = new JSONObject(transaction);
                    agg = new JSONObject(aggregate);
                    
                    transactionValue = a.getDouble("transactionValue");
                    transactionShares = a.getInt("shares");
                    volume = agg.getInt("shareVolume");
                    aggregatePrice = agg.getDouble("avgPrice");
                    
                    avg = (transactionValue + aggregatePrice*volume)/(transactionShares + volume);
                    
                    agg.remove("shareVolume");
                    agg.remove("value");
                    agg.remove("avgPrice");
                    agg.remove("stock");
                    agg.put("shareVolume", (transactionShares + volume));
                    agg.put("value", (transactionValue + aggregatePrice*volume));
                    agg.put("avgPrice", avg);
                    agg.put("stock", stock);
                    
                    if(a.getString("transactionType").compareTo("BUY") == 0){
                        int buy = agg.getInt("purchases");
                        agg.remove("purchases");
                        agg.put("purchases",++buy);
                    }
                    else{
                        int sales = agg.getInt("sales");
                        agg.remove("sales");
                        agg.put("sales",++sales);
                    }
                    
                    return agg.toString();
                }
            },Materialized.with(Serdes.String(), Serdes.String()))
            .toStream()
            .to("stock_volume");

        stockVolume
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
            .aggregate(new Initializer<String>(){
                @Override
                public String apply(){
                    JSONObject stockVolume = new JSONObject();
                    stockVolume.put("stock", "");
                    stockVolume.put("shareVolume", 0);
                    stockVolume.put("avgPrice", 0.0);
                    return stockVolume.toString();
                }
            }, new Aggregator<String, String, String>() {
                JSONObject a, agg;
                Double avg, aggregatePrice, transactionValue;
                int transactionShares, volume; 

                @Override
                public String apply(String stock, String transaction, String aggregate){
                    a = new JSONObject(transaction);
                    agg = new JSONObject(aggregate);
                    
                    transactionValue = a.getDouble("transactionValue");
                    transactionShares = a.getInt("shares");
                    volume = agg.getInt("shareVolume");
                    aggregatePrice = agg.getDouble("avgPrice");
                    
                    avg = (transactionValue + aggregatePrice*volume)/(transactionShares + volume);
            
                    agg.put("shareVolume", transactionShares+volume);
                    agg.put("avgPrice", avg);
                    agg.put("stock", stock);
                    
                    return agg.toString();
                }
            },Materialized.with(Serdes.String(), Serdes.String()))
            .toStream()
            .to("stock_window");
        
        validatedCount = validated
            .groupByKey()
            .count();

        validated
            .groupBy((key,value) -> "validCount")
            .count()
            .toStream()
            .to("validated_requests_cnt", Produced.with(Serdes.String(), Serdes.Long()));

        invalidated = builder.stream("invalidated_transaction_request");        
        invalidated
            .groupBy((key,value) -> "invalidCount")
            .count()
            .toStream()
            .to("invalidated_requests_cnt", Produced.with(Serdes.String(), Serdes.Long()));

        invalidatedCount = invalidated
            .groupByKey()
            .count()
            .toStream(); 
        
        invalidatedCount.join(validatedCount,
            (invalidcnt, validcnt) -> {
                Double ratio, valid = (double)validcnt, invalid = (double)invalidcnt;
                ratio =  valid/(valid+invalid);
                JSONObject agg = new JSONObject();
                agg.put("invalid", invalidcnt);
                agg.put("valid", validcnt);
                agg.put("ratio", ratio);
                return agg.toString();
            }).to("transaction_ratios");

        dag = builder.build();
        System.out.println(dag.describe().toString());
        return dag;
    }
    private void addShutdownHook(CountDownLatch latch){
        Runtime.getRuntime()
               .addShutdownHook(
                    new Thread(() ->
                    { 
                        stream.close();
                        latch.countDown();
                    })
               );
    }
    public static void main(String[] args) {
        CountDownLatch latch = new CountDownLatch(1);
        try{
            Fabric fabric = new Fabric();
            fabric.addShutdownHook(latch);
            fabric.stream.start();
            latch.await();
            fabric.stream.cleanUp();
        }
        catch(Throwable e){
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}