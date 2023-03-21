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
    final static String USERDIR = System.getProperty("user.dir");
    private MongoClient mongo;
    private MongoDatabase db;
    private MongoCollection<Account> accounts;
    private TreeMap<String,ObjectId> map;
    private KafkaConsumer<String,String> consumer;
    private KafkaProducer<String,String> producer;
    //private Transactions transactions;
    
    public Brokerage() throws IOException, FileNotFoundException{
        configureStorage();
        map = new TreeMap<String, ObjectId>();
        
        Properties cprops = new Properties();;
        cprops.load(new FileInputStream(USERDIR+"/configs/consumerConfig"));
        consumer = new KafkaConsumer<String,String>(cprops);
        consumer.subscribe(Arrays.asList("validated_transaction_requests"));
        
        Properties pprops = new Properties();
        pprops.load(new FileInputStream(USERDIR+"/configs/brokerProducerConfig"));
        producer = new KafkaProducer<String,String>(pprops);
        createAccounts();
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
    public void createAccounts() throws IOException{
        //String[] names = Files.readAllLines(Paths.get(Brokerage.USERDIR + "/data/names.csv")).toArray(new String[0]);
        //for(String name : names){
        String name = "Alexander Parsons";
        Account a = new Account(name);
        a.credit(Math.random()*25000.0 + 10000.0);
        a.setAccountValue(a.getCash());
        accounts.insertOne(a);
        System.out.println(a.getId());
        map.put(name, a.getId());
        JSONObject jo = new JSONObject(a);
        System.out.println(jo.toString());
        ProducerRecord<String,String> record = new ProducerRecord<String,String>("processed_transactions", name, new JSONObject(a).toString());
        producer.send(record);
     //   }
    }
    protected void processTransactions(){
        ConsumerRecords<String,String> records;
        JSONObject jo;
        while(true){
            records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record : records){
                jo = new JSONObject(record.value());
                if(jo.getString("transactionType").compareTo("BUY") == 0){
                    purchase(jo);
                }
                else sell(jo);
            }
        }
    }
    private void purchase(JSONObject transactionDetails){
        String acctHolder = transactionDetails.getString("name");
        String symbol = transactionDetails.getString("symbol");
        int shares = transactionDetails.getInt("shares");
        Double price = transactionDetails.getDouble("price");
        Double transactionValue = transactionDetails.getDouble("transactionValue");
        ProducerRecord<String,String> record;
        Account acct = accounts.find(eq("_id",map.get(acctHolder))).cursor().next();
  
        if(acct.getPositions().containsKey(symbol)){
            Position p = acct.getPositions().get(symbol);
            Double basis = p.getCostBasis();
            Double updatedBasis;
            int sharesInPosition = p.getShares();
            updatedBasis = (basis*sharesInPosition + transactionValue)/(sharesInPosition + shares);
            p.setGain((basis-price)*sharesInPosition);
            p.setCostBasis(updatedBasis);
            p.buy(shares, transactionValue);
            acct.debit(transactionValue);
            accounts.findOneAndReplace(eq("_id", map.get(acctHolder)), acct);
        }
        else{
            Position p = new Position(new Stock(symbol, transactionDetails.getString("companyName"), price, transactionDetails.getString("type")), shares);
            acct.addPosition(symbol, p);
            acct.debit(transactionValue);
            accounts.findOneAndReplace(eq("_id", map.get(acctHolder)),acct);
        }
        record = new ProducerRecord<String,String>("processed_transactions", acctHolder, new JSONObject(acct).toString());
        producer.send(record);
    }
    private void sell(JSONObject transactionDetails){
        String acctHolder = transactionDetails.getString("name");
        String symbol = transactionDetails.getString("symbol");
        int shares = transactionDetails.getInt("shares");
        Double transactionValue = transactionDetails.getDouble("transactionValue");
        Double price = transactionDetails.getDouble("price");
        ProducerRecord<String,String> record;
        
        Account acct = accounts.find(eq("_id", map.get(acctHolder))).cursor().next();
        Position p = acct.getPositions().get(symbol);
        
        p.getStock().setPrice(price);
        Double transactionGainLoss = (price-p.getCostBasis())*shares;
        Double gain = transactionGainLoss + p.getGain();
        p.setGain(gain);
        p.sell(shares, transactionValue);
        if(p.getShares() == 0){
            acct.getPositions().remove(symbol);
        }
        acct.credit(transactionValue);
        accounts.findOneAndReplace(eq("_id", map.get(acctHolder)), acct);
        record = new ProducerRecord<String,String>("processed_transactions", acctHolder, new JSONObject(acct).toString());
        producer.send(record);
    }

    public static void main(String[] args){
        try{
            Brokerage brokerage = new Brokerage();
            //brokerage.transactions = new Transactions();
            //brokerage.transactions.produceTransactions();
            CountDownLatch latch = new CountDownLatch(2);
            brokerage.setupShutdownHook(latch);
            brokerage.processTransactions();
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