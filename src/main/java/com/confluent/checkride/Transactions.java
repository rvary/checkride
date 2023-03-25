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

public class Transactions {
    final String TOPIC = "transaction_request";
    final String USERDIR = System.getProperty("user.dir");
    final int TRANSACTION_CAP = 100;
    Data data;
    JSONArray ja;
    Properties properties;
    KafkaProducer<String,String> producer; 
    public Transactions() throws IOException{
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
    private Double getRandomNumber(Double min, Double max){
        return (Double)(Math.random()*(max-min) + min);
    }
    protected void test(int numShares, String transactionType, Double price){
        try{
            //produceTransactions(numShares, transactionType, price);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    protected void produceTransactions() throws InterruptedException, ExecutionException{
        int nInd, sInd, shares;
        Double value, basisOffset;
        String transactionType;
        BigDecimal bd;
        DecimalFormat df = new DecimalFormat("#.##");
        JSONObject jo;
        for(int j = 0; j < 50; j++){
            nInd = (int)(Math.random()*(data.names.size()-1));
            //sInd = (int)(Math.random()*(ja.length()-1));
            sInd = 0;
            shares = (int)(Math.random()*(TRANSACTION_CAP));
            transactionType = ((int)(Math.random()*(2)) == 1) ? "BUY" : "SELL"; 
            //transactionType = type;
            basisOffset = j > 1 ? getRandomNumber(0.8,1.2) : 1.0;
            jo = (JSONObject)ja.get(sInd);
            //jo.remove("price");
            //jo.put("price", price.toString());
            bd = new BigDecimal(jo.getDouble("price")*basisOffset);
            value =  shares*bd.doubleValue();
            
            //jo.put("name", data.names.get(nInd));
            jo.put("name","Alexander Parsons");
            jo.put("shares", shares);
            jo.put("transactionValue", df.format(value));
            jo.put("transactionType", transactionType);
            System.out.println(jo);
              
            ProducerRecord<String,String> record = new ProducerRecord<String,String>(TOPIC,jo.getString("name"),jo.toString());
            producer.send(record);     
        }
    }
    public static void main(String[] args) throws IOException{
        Transactions transactions = new Transactions();
        final CountDownLatch latch = new CountDownLatch(1);
        try{
            transactions.setupShutdownHook(latch);
            transactions.produceTransactions();
            latch.await();
        
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}