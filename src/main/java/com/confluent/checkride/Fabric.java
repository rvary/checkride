package com.confluent.checkride;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.json.JSONObject;
import org.apache.kafka.streams.Topology;

import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.io.FileInputStream;
import java.io.IOException;

public class Fabric {
    final String USERDIR = System.getProperty("user.dir") + "/configs/";
    private KafkaStreams stream;

    public Fabric() throws IOException{
        Properties props = new Properties();
        props.load(new FileInputStream(USERDIR + "brokerStreamConfig"));
        stream = new KafkaStreams(getTopology(), props);
    }
    private Topology getTopology(){
        Topology dag;
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String,String> processed, enriched, validated, invalidated;
        KTable<String,String> brokerageAccounts;
        KTable<String, Long> validatedCnt, invalidatedCnt;
        Produced<String,Long> p1 = Produced.with(Serdes.String(),Serdes.Long());
        Produced<String,Long> p2 = Produced.with(Serdes.String(),Serdes.Long());
           
        processed = builder.stream("processed_transaction"); 
        
        processed.mapValues(v -> {
            JSONObject jo = new JSONObject();
            JSONObject record = new JSONObject(v);
            String holder = record.getString("holder");
            jo.put("account:", holder);
            jo.put("accountValue", record.getBigDecimal("accountValue").toString());
            return jo.toString();
        }).to("brokerage_account_values");

        brokerageAccounts = processed.toTable();
        brokerageAccounts.toStream().to("brokerage_accounts");
        
        enriched = builder.stream("enriched_transaction_request");

        validated = enriched.filter(new Predicate<String,String>(){
            @Override
            public boolean test(String key, String value){
                JSONObject request = new JSONObject(value);
                JSONObject acct = new JSONObject(request.get("account").toString());
                if(request.getString("transactionType").compareTo("BUY") == 0){
                    return acct.getDouble("cash") >= request.getDouble("transactionValue") ? true : false;
                }
                else{
                    JSONObject position, stock;
                    JSONObject positions = new JSONObject(acct.get("positions").toString());
                    Iterator<String> stocks = positions.keys();
                    String symbol;
                    while(stocks.hasNext()){
                        symbol = stocks.next();
                        position = new JSONObject(positions.get(symbol).toString());
                        stock = new JSONObject(position.get("stock").toString());
                        if(stock.getString("symbol").compareTo(symbol) == 0){
                            return position.getInt("shares") >= request.getInt("shares") ? true : false;
                        }
                    }
                    return false;
                }
            }
        });
        validated.to("validated_transaction_request");
        validatedCnt = validated.groupByKey().count();
        validatedCnt.toStream().to("account_holders_validated_transactions", p1);
        
        invalidated = enriched.filter(new Predicate<String,String>(){
            @Override
            public boolean test(String key, String value){
                JSONObject request = new JSONObject(value);
                JSONObject acct = new JSONObject(request.get("account").toString());
                if(request.getString("transactionType").compareTo("BUY") == 0){
                    return request.getDouble("transactionValue") > acct.getDouble("cash") ? true : false;
                }
                else{
                    JSONObject position, stock; 
                    JSONObject positions = new JSONObject(acct.get("positions").toString());
                    Iterator<String> stocks = positions.keys();
                    String symbol;
                    while(stocks.hasNext()){
                        symbol = stocks.next();
                        position = new JSONObject(positions.get(symbol).toString());
                        stock = new JSONObject(position.get("stock").toString());
                        if(stock.getString("symbol").compareTo(symbol) == 0){
                            return position.getInt("shares") < request.getInt("shares") ? true : false;
                        }
                    }
                    return true;
                }
            }
        })
        .mapValues(new ValueMapperWithKey<String,String,String>() {
           @Override
           public String apply(String acctName, String transaction){
                JSONObject message = new JSONObject(transaction);
                if(message.getString("transactionType").compareTo("BUY") == 0){
                    message.put("message", "Insufficient Funds");
                }
                else{
                    message.put("message", "Account doesn't hold " + message.getInt("shares") + 
                        " shares of " + message.getString("symbol"));
                }
                return message.toString();   
           }
        });
        invalidatedCnt = invalidated.groupByKey().count();
        invalidatedCnt.toStream().to("account_holders_invalidated_transactions", p2);
        invalidated.to("invalidated_transaction_request");
        
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
        }
        catch(Throwable e){
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}