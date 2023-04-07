package com.confluent.checkride;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
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
    private MongoClient mongo;
    private MongoDatabase db;
    private MongoCollection<Account> accounts;
    private TreeMap<String,ObjectId> map;
    private KafkaConsumer<String,String> consumer;
    private KafkaProducer<String,String> producer;
   
    public Brokerage(){
        configureStorage();  
        loadProperties();
        map = new TreeMap<String, ObjectId>();
    }
    private void loadProperties(){
        Properties consumerProperties, producerProperties;
        try{
            consumerProperties = new Properties();;
            consumerProperties.load(new FileInputStream(System.getProperty("user.dir") + "/configs/brokerageConsumer"));
            consumer = new KafkaConsumer<String,String>(consumerProperties);
            consumer.subscribe(Arrays.asList("transaction_request"));

            producerProperties = new Properties();
            producerProperties.load(new FileInputStream(System.getProperty("user.dir") + "/configs/brokerageProducer"));
            producer = new KafkaProducer<String,String>(producerProperties);
        }catch(IOException e){
            e.printStackTrace();
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
    protected void createAccount() throws IOException{
        String[] names = Files.readAllLines(Paths.get(System.getProperty("user.dir") 
            + "/data/names.csv")).toArray(new String[0]);
        for(String name : names){
            Account a = new Account(name);
            //a.setCash(30000.22);
            a.setCash(Math.random()*35000.0 + 10000.0);
            a.setAccountValue(a.getCash());
            accounts.insertOne(a);
            map.put(name, a.getId());
        }
    }
    private void processRequest(){
        ConsumerRecords<String,String> requests;
        ProducerRecord<String,String> invalidatedRecord, validatedRecord, processedRecord;
        Double transactionValue, accountCash;
        String holder, transactionType, symbol;
        TreeMap<String,Position> positions;
        int transactionShares, positionShares;
        Account account;
        JSONObject request;
        while(true){
            requests = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record : requests){

                if(record.topic().compareTo("create_account_request") == 0){
                    try{
                        createAccount();
                    }catch(Exception e){};
                    continue;
                }

                request = new JSONObject(record.value());

                symbol = request.getString("symbol");
                holder = request.getString("name");
                
                account = accounts.find(eq("_id",map.get(holder))).cursor().next();  
                accountCash = account.getCash();
                positions = account.getPositions();
                
                positionShares = positions.containsKey(symbol) ? positions.get(symbol).getShares() : 0;
                
                transactionShares = request.getInt("shares");
                transactionValue = request.getDouble("transactionValue");
                transactionType = request.getString("transactionType");

                if(transactionType.compareTo("BUY") == 0){
                    if(accountCash > transactionValue){
                        validatedRecord = new ProducerRecord<String,String>("validated_transaction_request", holder, request.toString()); 
                        producer.send(validatedRecord);
                        account = purchase(request, account);
                        processedRecord = new ProducerRecord<String,String>("processed_transaction", holder, new JSONObject(account).toString());
                        producer.send(processedRecord);
                    }
                    else{
                        request.put("message", "Insufficient funds");
                        request.put("account", account.toString());
                        invalidatedRecord = new ProducerRecord<String,String>("invalidated_transaction_request", holder,request.toString());
                        producer.send(invalidatedRecord);
                    }
                }
                else if(positionShares >= transactionShares){
                        validatedRecord = new ProducerRecord<String,String>("validated_transaction_request", holder, request.toString()); 
                        producer.send(validatedRecord);
                        account = sell(request, account);
                        processedRecord = new ProducerRecord<String,String>("processed_transaction", holder, new JSONObject(account).toString());
                        producer.send(processedRecord);
                }
                else{
                    invalidatedRecord = new ProducerRecord<String,String>("invalidated_transaction_request", holder,request.toString());
                    producer.send(invalidatedRecord);
                }
            }
        }
    }
    private Account purchase(JSONObject transactionDetails, Account account){
        String acctHolder = transactionDetails.getString("name");
        String symbol = transactionDetails.getString("symbol");
        Double price = transactionDetails.getDouble("price");
        Double transactionValue = transactionDetails.getDouble("transactionValue");
        int transactionShares = transactionDetails.getInt("shares");
        DecimalFormat df = new DecimalFormat("#.0000");
  
        if(account.getPositions().containsKey(symbol)){
            Position p = account.getPositions().get(symbol);
            Double basis = p.getCostBasis();
            Double updatedBasis;
            Double positionGain;

            //update the stock's price
            p.getStock().setPrice(price);
            
            //basis is a weighted average of the position's shares, their cost basis and the shares purchased in this transaction
            updatedBasis = Double.parseDouble(df.format((basis*p.getShares() + transactionValue)/(p.getShares() + transactionShares)));
            p.setCostBasis(updatedBasis);
            
            //buy the shares
            p.buy(transactionShares);

            //compute gain based on current price before buying shares
            positionGain = Double.parseDouble(df.format((price-p.getCostBasis())*p.getShares()));

            //acct needs to reflect updated change in position gain and value based on transaction; remove previous position gain and value
            account.setAccountGain(Double.parseDouble(df.format(account.getAccountGain() - p.getGain())));
            account.setAccountValue(Double.parseDouble(df.format(account.getAccountValue() - p.getPositionValue())));

            //buy the shares, set the position's gain and value
            p.setGain(positionGain);
            p.setPositionValue(Double.parseDouble(df.format(p.getShares()*p.getStock().getPrice())));
            
            //debit the accout and reset the account's gain and value
            account.setCash(Double.parseDouble(df.format(account.getCash() - transactionValue)));            
            account.setAccountGain(Double.parseDouble(df.format(account.getAccountGain() + p.getGain())));
            account.setAccountValue(Double.parseDouble(df.format(account.getAccountValue() - transactionValue + p.getPositionValue())));
            
            //update the mongo document
            accounts.findOneAndReplace(eq("_id", map.get(acctHolder)), account);
        }
        else{
            //If position doesn't exist, add position to account, debit the account, update the account's value and mongo document.  
            //The position's value is set in the constructor, which is based on the current price and number of shares purchased.
            Position p = new Position(new Stock(symbol, transactionDetails.getString("companyName"), price, 
                transactionDetails.getString("type")), transactionShares);
            account.addPosition(symbol, p);
            account.setCash(Double.parseDouble(df.format(account.getCash() - transactionValue)));
            //pedantic
            account.setAccountValue(Double.parseDouble(df.format(account.getAccountValue() - transactionValue + p.getPositionValue())));
            accounts.findOneAndReplace(eq("_id", map.get(acctHolder)),account);
        }
        return account;
    }
    private Account sell(JSONObject transactionDetails, Account account){
        String acctHolder = transactionDetails.getString("name");
        String symbol = transactionDetails.getString("symbol");
        Double transactionValue = transactionDetails.getDouble("transactionValue");
        Double price = transactionDetails.getDouble("price");
        Double transactionGainLoss;
        Double positionGain;
        int transactionShares = transactionDetails.getInt("shares");
        DecimalFormat df = new DecimalFormat("#.0000");

        Position p = account.getPositions().get(symbol);
        p.sell(transactionShares);
        p.getStock().setPrice(Double.parseDouble(df.format(price)));
        
        transactionGainLoss = Double.parseDouble(df.format((p.getStock().getPrice()-p.getCostBasis())*transactionShares));
        positionGain = Double.parseDouble(df.format((p.getStock().getPrice()-p.getCostBasis())*p.getShares()));

        //To update account gain/value correctly, remove position's gain and value before the transaction occurs
        account.setAccountGain(Double.parseDouble(df.format(account.getAccountGain() - p.getGain())));
        account.setAccountValue(Double.parseDouble(df.format(account.getAccountValue() - p.getPositionValue())));

        p.setGain(positionGain);
        p.setPositionValue(Double.parseDouble(df.format(p.getShares()*p.getStock().getPrice())));
        
        //remove the position if all shares have been sold
        if(p.getShares() == 0){
            account.getPositions().remove(symbol);
        }
        account.setCash(Double.parseDouble(df.format(account.getCash() + transactionValue)));
        account.setAccountGain(Double.parseDouble(df.format(account.getAccountGain() + transactionGainLoss + p.getGain())));
        account.setAccountValue(Double.parseDouble(df.format(account.getAccountValue() + transactionValue + p.getPositionValue())));
        accounts.findOneAndReplace(eq("_id", map.get(acctHolder)), account);
        return account;
    }
    public static void main(String[] args){
        try{
            Brokerage brokerage = new Brokerage();
            brokerage.createAccount();
            CountDownLatch latch = new CountDownLatch(2);
            brokerage.setupShutdownHook(latch);
            brokerage.processRequest();
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