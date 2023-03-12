package com.confluent.checkride;
import java.util.Properties;
import java.io.*;
import org.apache.kafka.clients.producer.KafkaProducer;

public class TransactionProducer {
    Data data;
    Properties properties;
    KafkaProducer<String,String> producer; 
    public TransactionProducer() throws IOException{
        data = new Data(false);
        properties = new Properties();
        properties.load(new FileInputStream(System.getProperty("user.dir") + "/configs/producerConfig"));
        producer = new KafkaProducer<>(properties);
    }
    public void generateTransactions(){
        int ind;
        for(int j = 0; j < 10; j++){
            ind = (int) (Math.random()*(data.stocks.size()-1));
            System.out.println(data.stocks.get(ind));
        }
    }
    public static void main(String[] args){
        
    }
}