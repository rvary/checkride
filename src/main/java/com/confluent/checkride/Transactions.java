package com.confluent.checkride;

import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.io.*;
import java.text.DecimalFormat;

import org.json.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Transactions {
    Data data;
    JSONArray ja;
    Properties properties;
    KafkaProducer<String,String> producer; 
    TreeMap<String,Double> prices;
    public Transactions() throws IOException{
        data = new Data();
        data.getNames();
        prices = new TreeMap<String,Double>();
        
        ja = data.readJSON("nasdaq.json");
        for(int j = 0; j < prices.size(); j++){
            prices.put(ja.getJSONObject(j).getString("symbol"), ja.getJSONObject(j).getDouble("price"));
        }
        
        properties = new Properties();
        properties.load(new FileInputStream(System.getProperty("user.dir") + "/configs/producerConfig"));
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
    protected void produceTransactions() throws InterruptedException, ExecutionException{
        int nInd, sInd, shares;
        Double value, price, basisOffset;
        String transactionType;
        DecimalFormat df = new DecimalFormat("#.00");
        JSONObject jo;
        for(int j = 0; j < 5000; j++){
            
            Thread.sleep((int)(50));

            nInd = (int)(Math.random()*(data.names.size()-1));
            sInd = (int)(Math.random()*(ja.length()-1));
            
            transactionType = ((int)(Math.random()*(2)) == 1) ? "BUY" : "SELL"; 
            basisOffset = getRandomNumber(0.99,1.01);

            if(transactionType.compareTo("SELL") == 0){
                do{
                    shares = (int)(Math.random()*(50));
                }while(shares == 0);
            }
            else{
                do{
                    shares = (int)(Math.random()*(100));
                }while(shares == 0);
            }
           
            jo = ja.getJSONObject(sInd);
            price = Double.parseDouble(df.format(jo.getDouble("price")*basisOffset));
            jo.remove("price");
            jo.put("price", price);
            prices.replace(jo.getString("symbol"), price);
            value =  Double.parseDouble(df.format(jo.getDouble("price")*shares));
            
            jo.put("name", data.names.get(nInd));
            jo.put("shares", shares);
            jo.put("transactionValue", df.format(value));
            jo.put("transactionType", transactionType);
            
            System.out.println(jo);
              
            ProducerRecord<String,String> record = new ProducerRecord<String,String>("transaction_request",jo.getString("name"),jo.toString());
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