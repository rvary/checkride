package com.confluent.checkride;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.kafka.streams.Topology;

import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.io.FileInputStream;
import java.io.IOException;

public class DataFabric {
    final String USERDIR = System.getProperty("user.dir");
    private KafkaStreams streams;

    public DataFabric() throws IOException{
        Properties props = new Properties();
        props.load(new FileInputStream(USERDIR + "/configs/streamsConfig"));
        streams = new KafkaStreams(getTopology(), props);
    }
    private Topology getTopology(){
        Topology dag;
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String,String> requestedTransactions, processedTransactions, enrichedTransactionRequest;
        KStream<String,String> validatedTransactions, invalidatedTransactions;
        KTable<String,String> brokerageAccounts;
        KTable<String, Long> validTransactionCnt, invalidTransactionCnt;
        Produced<String,Long> produced = Produced.with(Serdes.String(),Serdes.Long());
   
        requestedTransactions = builder.stream("transaction_requests");   
        
        processedTransactions = builder.stream("processed_transactions");
        
        processedTransactions.mapValues(v -> {
            JSONObject jo = new JSONObject();
            JSONObject record = new JSONObject(v);
            jo.put("account:", record.getString("holder"));
            jo.put("accountValue", record.getBigDecimal("accountValue").toString());
            return jo.toString();
        }).to("brokerage_account_values");
                
        brokerageAccounts = processedTransactions.toTable();
        brokerageAccounts.toStream().to("brokerage_accounts");
        enrichedTransactionRequest = requestedTransactions
        .join(brokerageAccounts, new ValueJoiner<String,String,String>(){
            @Override
            public String apply(String request, String account){
                System.out.println(account);
                JSONObject acct = new JSONObject(account);
                JSONObject transactionRequest = new JSONObject(request);
                transactionRequest.put("account",acct);
                return transactionRequest.toString();
            }
        });
       // enrichedTransactionRequest.to("enrichedTransactionRequests");
         
        validatedTransactions = enrichedTransactionRequest.filter(new Predicate<String,String>(){
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
        validatedTransactions.to("validated_transaction_requests");
        validTransactionCnt = processedTransactions.groupByKey().count();
        validTransactionCnt.toStream().to("account_holders_validated_transactions", produced);
        
        invalidatedTransactions = enrichedTransactionRequest.filter(new Predicate<String,String>(){
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
                        if(stock.getString("symbol").compareTo("symbol") == 0){
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
                    message.remove("account");
                }
                else{
                    message.put("message", message.getString("symbol") + " position doesn't exist.");
                }
                return message.toString();   
           }
        });
        invalidTransactionCnt = invalidatedTransactions.groupByKey().count();
        invalidTransactionCnt.toStream().to("account_holders_invalidated_transactions", produced);
        //invalidatedTransactions.to("invalidated_transaction_requests");
        
        dag = builder.build();
        System.out.println(dag.describe().toString());
        return dag;
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
    private void start(){
        streams.start();
    }
    public static void main(String[] args) {
        CountDownLatch latch = new CountDownLatch(1);
        try{
            DataFabric fabric = new DataFabric();
            fabric.addShutdownHook(latch);
            System.out.println("STARTING");
            fabric.start();
            latch.await();
        }
        catch(Throwable e){
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}