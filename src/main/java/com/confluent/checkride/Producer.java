package com.confluent.checkride;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.io.*;
import java.text.DecimalFormat;
import java.math.BigDecimal;

import org.json.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {
    final String TOPIC = "transactions";
    final String USERDIR = System.getProperty("user.dir");
    final int TRANSACTION_CAP = 1000;
    Data data;
    JSONArray ja;
    Properties properties;
    KafkaProducer<String,String> producer; 
    public Producer() throws IOException{
        data = new Data(false);
        ja = data.readJSON("largecap.json");
        properties = new Properties();
        properties.load(new FileInputStream(USERDIR + "/configs/producerConfig"));
        producer = new KafkaProducer<>(properties);
    }
    private void setupShutdownHook(CountDownLatch latch){
        Runtime.getRuntime()
                .addShutdownHook(
                    new Thread(() -> {
                        producer.close();
                        latch.countDown();
                    }
                ));
    }
    private void produceTransactions() throws InterruptedException, ExecutionException{
        int nInd, sInd, shares;
        Double value;
        String transactionType;
        BigDecimal bd;
        DecimalFormat df = new DecimalFormat("#.##");;
        JSONObject jo;
        for(int j = 0; j < 10; j++){
            nInd = (int)(Math.random()*(data.names.size()-1));
            sInd = (int)(Math.random()*(ja.length()-1));
            shares = (int)(Math.random()*(TRANSACTION_CAP));
            transactionType = ((int)(Math.random()*(2)) == 1) ? "BUY" : "SELL"; 
            
            jo = (JSONObject)ja.get(sInd);
            bd = (BigDecimal)jo.get("price");
            value =  shares*bd.doubleValue();
            
            jo.put("name", data.names.get(nInd));
            jo.put("shares:", shares);
            jo.put("value", df.format(value));
            jo.put("transaction:", transactionType);
            //System.out.println(jo);

            ProducerRecord<String,String> record = new ProducerRecord<String,String>(TOPIC,jo.getString("symbol"),jo.toString());
            System.out.println("SENDING RECORD!!");
            //producer.send(record).get();
            
            producer.send(record, (md, e) ->{
                if(e != null){
                    e.printStackTrace();
                }
                else{
                    System.out.printf("KEY: %s, VALUE: %s, Topic: %s, Partition: %s, Offset: %s\n", record.key(), record.value(), 
                        md.topic(), md.partition(), md.offset());
                }
            });      

        }
    }
    public static void main(String[] args) throws IOException{
    
        Producer producer = new Producer();
        final CountDownLatch latch = new CountDownLatch(1);
        try{
            producer.setupShutdownHook(latch);
            producer.produceTransactions();
            latch.await();
        
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}