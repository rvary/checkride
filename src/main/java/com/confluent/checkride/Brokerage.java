package com.confluent.checkride;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.types.ObjectId;
import org.json.JSONObject;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.util.Properties;

public class Brokerage {
    final static String USERDIR = System.getProperty("user.dir") + "/configs/";
    private MongoClient mongo;
    private MongoDatabase db;
    private MongoCollection<Account> accounts;
    private TreeMap<String,ObjectId> map;
    private KafkaConsumer<String,String> consumer, request;
    private KafkaProducer<String,String> producer, requestProducer;
    ConsumerRecords<String,String> requestRecords, validatedRequestRecords;
   
    public Brokerage() throws IOException, FileNotFoundException{
        configureStorage();
        
        map = new TreeMap<String, ObjectId>();
        
        Properties c1Properties, c2Properties, pProperties;

        c1Properties = new Properties();;
        c1Properties.load(new FileInputStream(USERDIR+"brokerConsumer1Config"));
        consumer = new KafkaConsumer<String,String>(c1Properties);
        consumer.subscribe(Arrays.asList("validated_transaction_request","invalidated_transaction_request"));
        
        c2Properties = new Properties();
        c2Properties.load(new FileInputStream(USERDIR+"brokerConsumer2Config"));
        request = new KafkaConsumer<String,String>(c2Properties);
        request.subscribe(Arrays.asList("transaction_request"));

        pProperties = new Properties();
        pProperties.load(new FileInputStream(USERDIR+"brokerProducerConfig"));
        producer = new KafkaProducer<String,String>(pProperties);
        requestProducer = new KafkaProducer<String,String>(pProperties);
    }
    private void run(){
        while(true){
            processRequest();
            processTransactions();
        }
    }
    private void setupShutdownHook(CountDownLatch latch){
        Runtime.getRuntime()
               .addShutdownHook(
                    new Thread(() -> {
                        consumer.close();
                        latch.countDown();
                        producer.close();
                        latch.countDown();
                    })
               );
    }
    private void configureStorage(){
        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
            fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        MongoClientSettings settings = MongoClientSettings.builder().codecRegistry(pojoCodecRegistry).build();
        mongo = MongoClients.create(settings);
        db = mongo.getDatabase("brokerage").withCodecRegistry(pojoCodecRegistry);
        accounts = db.getCollection("accounts",Account.class);
    }
    private void createAccounts() throws IOException{
        //String[] names = Files.readAllLines(Paths.get(Brokerage.USERDIR + "/data/names.csv")).toArray(new String[0]);
        //for(String name : names){
        String name = "Alexander Parsons";
        Account a = new Account(name);
        a.setCash(30000.22);
        //a.setCash(Math.random()*25000.0 + 10000.0);
        a.setAccountValue(a.getCash());
        accounts.insertOne(a);
        System.out.println(a.getId());
        map.put(name, a.getId());
     //   }
    }
    private void processRequest(){
        ConsumerRecords<String,String> records;
        ConsumerRecord<String,String> record;
        Account acct;
        JSONObject jo, account;
        String holder;

        do{
            records = request.poll(Duration.ofMillis(1)); 
        }while(records.count() == 0); 
 
        record = records.iterator().next();
        
        jo = new JSONObject(record.value());
        holder = jo.getString("name");
        
        acct = accounts.find(eq("_id",map.get(holder))).cursor().next();
        account = new JSONObject(acct);
        
        jo.put("account", account);
        
        ProducerRecord<String,String> enrichedRequest = new ProducerRecord<String,String>("enriched_transaction_request", holder, jo.toString());
        requestProducer.send(enrichedRequest);
    }
    private void processTransactions(){
        JSONObject jo;
        ConsumerRecords<String,String> records;
        ConsumerRecord<String,String> record;

        do{
            records = consumer.poll(Duration.ofMillis(1));
        }while(records.count() == 0);

        record = records.iterator().next();
        
        if(record.topic().compareTo("invalidated_transaction_request") == 0) return;

        jo = new JSONObject(record.value());
  
        if(jo.getString("transactionType").compareTo("BUY") == 0){
            System.out.println("PURCHASE");
            purchase(jo);
        }
        else 
        {
            System.out.println("SALE");
            sell(jo);
        }
    }
    private void purchase(JSONObject transactionDetails){
        String acctHolder = transactionDetails.getString("name");
        String symbol = transactionDetails.getString("symbol");
        Double price = transactionDetails.getDouble("price");
        Double transactionValue = transactionDetails.getDouble("transactionValue");
        int transactionShares = transactionDetails.getInt("shares");
        
        ProducerRecord<String,String> record;
        Account acct = accounts.find(eq("_id",map.get(acctHolder))).cursor().next();
  
        if(acct.getPositions().containsKey(symbol)){
            Position p = acct.getPositions().get(symbol);
            Double basis = p.getCostBasis();
            Double updatedBasis;
            Double positionGain;

            //update the stock's price
            p.getStock().setPrice(price);
            
            //basis is a weighted average of the position's shares, their cost basis and the shares purchased in this transaction
            updatedBasis = (basis*p.getShares() + transactionValue)/(p.getShares() + transactionShares);
            p.setCostBasis(updatedBasis);
            
            //buy the shares
            p.buy(transactionShares);

            //compute gain based on current price before buying shares
            positionGain = (price-p.getCostBasis())*p.getShares();

            //acct needs to reflect updated change in position gain and value based on transaction; remove previous position gain and value
            acct.setAccountGain(acct.getAccountGain() - p.getGain());
            acct.setAccountValue(acct.getAccountValue() - p.getPositionValue());

            //buy the shares, set the position's gain and value
            p.setGain(positionGain);
            p.setPositionValue(p.getShares()*p.getStock().getPrice());
            
            //debit the accout and reset the account's gain and value
            acct.setCash(acct.getCash() - transactionValue);
            acct.setAccountGain(acct.getAccountGain() + p.getGain());
            acct.setAccountValue(acct.getAccountValue() - transactionValue + p.getPositionValue());
            
            //update the mongo document
            accounts.findOneAndReplace(eq("_id", map.get(acctHolder)), acct);
        }
        else{
            //If position doesn't exist, add position to account, debit the account, update the account's value and mongo document.  
            //The position's value is set in the constructor, which is based on the current price and number of shares purchased.
            Position p = new Position(new Stock(symbol, transactionDetails.getString("companyName"), price, 
                transactionDetails.getString("type")), transactionShares);
            acct.addPosition(symbol, p);
            acct.setCash(acct.getCash() - transactionValue);
            //pedantic
            acct.setAccountValue(acct.getAccountValue() - transactionValue + p.getPositionValue());
            accounts.findOneAndReplace(eq("_id", map.get(acctHolder)),acct);
        }
        record = new ProducerRecord<String,String>("processed_transaction", acctHolder, new JSONObject(acct).toString());
        producer.send(record);
    }
    private void sell(JSONObject transactionDetails){
        String acctHolder = transactionDetails.getString("name");
        String symbol = transactionDetails.getString("symbol");
        Double transactionValue = transactionDetails.getDouble("transactionValue");
        Double price = transactionDetails.getDouble("price");
        Double transactionGainLoss;
        Double positionGain;
        int transactionShares = transactionDetails.getInt("shares");

        ProducerRecord<String,String> record;
        Account acct = accounts.find(eq("_id", map.get(acctHolder))).cursor().next();
        
        Position p = acct.getPositions().get(symbol);
        p.sell(transactionShares);
        p.getStock().setPrice(price);
        
        transactionGainLoss = (p.getStock().getPrice()-p.getCostBasis())*transactionShares;
        positionGain = (p.getStock().getPrice()-p.getCostBasis())*p.getShares();

        //To update account gain/value correctly, remove position's gain and value before the transaction occurs
        acct.setAccountGain(acct.getAccountGain() - p.getGain());
        acct.setAccountValue(acct.getAccountValue() - p.getPositionValue());

        p.setGain(positionGain);
        p.setPositionValue(p.getShares()*p.getStock().getPrice());
        
        //remove the position if all shares have been sold
        if(p.getShares() == 0){
            acct.getPositions().remove(symbol);
        }

        acct.setCash(acct.getCash() + transactionValue);
        acct.setAccountGain(acct.getAccountGain() + transactionGainLoss + p.getGain());
        acct.setAccountValue(acct.getAccountValue() + transactionValue + p.getPositionValue());
        accounts.findOneAndReplace(eq("_id", map.get(acctHolder)), acct);
        record = new ProducerRecord<String,String>("processed_transaction", acctHolder, new JSONObject(acct).toString());
        producer.send(record);
    }

    public static void main(String[] args){
        try{
            Brokerage brokerage = new Brokerage();
            brokerage.createAccounts();
            //brokerage.transactions = new Transactions();
            //brokerage.transactions.produceTransactions();
            CountDownLatch latch = new CountDownLatch(2);
            brokerage.setupShutdownHook(latch);
            brokerage.run();
            latch.await();     
            brokerage.accounts.drop();
        }catch(final Throwable e){
            System.exit(1);
            e.printStackTrace();
            e.getCause();
        }
        System.exit(0);
    }
}