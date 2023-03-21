package com.confluent.checkride;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.json.JSONObject;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static com.mongodb.client.model.Filters.eq;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class Driver {
    public Driver(){}
    public static void main(String[] args) {
        Transactions transaction;
        Brokerage brokerage;
        BufferedReader br;
        String in;
        System.out.println("**START BROKERAGE and DATA FABRIC BEFORE TRIGGERING TRANSACTION**");
        try{
            transaction = new Transactions();
            brokerage = new Brokerage();
            br = new BufferedReader(new InputStreamReader(System.in));   
            while(true){
                in = br.readLine();
                if(in.compareTo("p") == 0){
                    System.out.println("producing transaction");
                    transaction.produceTransactions();
                }
                else if(in.compareTo("a") == 0){
                    System.out.println("generating account");
                    brokerage.createAccounts();
                }
                else if(in.compareTo("j") == 0){
                    System.out.println("processing transaction");
                    brokerage.processTransactions();
                }
            }
        }catch(final Throwable e){
            System.exit(1);
        }
        System.exit(0);
    }
}