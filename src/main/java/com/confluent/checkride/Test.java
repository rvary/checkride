package com.confluent.checkride;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

public class Test {
    KafkaConsumer<String,String> consumer;
    public Test(){}
    private void process(String holder){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"kafka:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple_consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "3");

        consumer = new KafkaConsumer<String,String>(props);
        consumer.subscribe(Arrays.asList("transaction_ratios"));
        
        ConsumerRecords<String,String> records;
        JSONObject transaction;

        while(true){
            records = consumer.poll(Duration.ofMillis(10)); 
            for(ConsumerRecord<String,String> record : records){
                if(record.key().compareTo(holder) == 0){
                    transaction = new JSONObject(record.value().toString());
                    System.out.println(transaction);
                }
            }
        }
    }
    private void verifyAccounts(){
        TreeMap<String,JSONObject> accounts = new TreeMap<String,JSONObject>();
        TreeMap<String,Double> initialDeposit = new TreeMap<String,Double>();
        String holder, data;
        JSONObject jo;
        try{
            BufferedReader br = new BufferedReader(new FileReader(System.getProperty("user.dir")+ "/data/out/brokerageAccounts.txt"));
            
            while((data = br.readLine()) != null){
        
                jo = new JSONObject(data);
                holder = jo.getString("holder");
                
                if(accounts.containsKey(holder)){
                    accounts.replace(holder, jo);
                }
                else {
                    accounts.put(holder, jo);
                    initialDeposit.put(holder, (jo.getDouble("accountValue")));
                }
            }

            br.close();

        }catch(Exception e){
            e.printStackTrace();
        }

        Double accountCash, accountGain, accountValue, positionsValue = 0.0;
        Map<String,Object> accountPositions;
        
        for(Map.Entry<String,JSONObject> account : accounts.entrySet()){
            
            jo = account.getValue();
            
            accountCash = jo.getDouble("cash");
            accountValue = jo.getDouble("accountValue");
            accountGain = jo.getDouble("accountGain");
            
            accountPositions = jo.getJSONObject("positions").toMap();

            for(Map.Entry<String,Object> positions : accountPositions.entrySet()){
                @SuppressWarnings("unchecked")
                HashMap<String,Object> map = (HashMap<String,Object>)positions.getValue();
                positionsValue += Double.parseDouble(map.get("positionValue").toString());
            }

            Double checkValue = accountCash + positionsValue, checkRecords = accountValue - accountGain;
            Double deposit = initialDeposit.get(account.getKey());
            assert (accountValue >= 0.999*checkValue && accountValue <= 1.001*checkValue) : jo.get("holder") + " accountCash + positionsValue != accountValue";
            assert (deposit >= 0.999*checkRecords && deposit <= 1.001*checkRecords) : jo.get("holder") + " accountValue - account Gain != initialDeposit";
            positionsValue = 0.0;
        }
    }
    private void verifyTransactions() {
        JSONObject jo;
        BufferedReader br;
        int transactions = 0, volumeTransactions = 0;
        String symbol, check, transaction;
        TreeMap<String,JSONObject> volume = new TreeMap<String,JSONObject>();
        try{
            br = new BufferedReader(new FileReader(System.getProperty("user.dir")+ "/data/out/validated.txt"));  
            while((check = br.readLine()) != null){
                transactions = Integer.parseInt(check);
            }

            br = new BufferedReader(new FileReader(System.getProperty("user.dir")+ "/data/out/stockVolume.txt"));
            
            while((transaction = br.readLine()) != null){
                jo = new JSONObject(transaction);
                symbol = jo.getString("stock");
                if(volume.containsKey(symbol)){
                    volume.replace(symbol, jo);
                }
                else{
                    volume.put(symbol,jo);
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        
        for(Map.Entry<String,JSONObject> stocks : volume.entrySet()){
            JSONObject stock = stocks.getValue();
            volumeTransactions += stock.getInt("sales") + stock.getInt("purchases");
            //System.out.printf("%s: sales: %d purchases: %d total: %d volume: %d\n", stock.get("stock"), stock.get("sales"), 
            //    stock.get("purchases"), stock.getInt("sales") + stock.getInt("purchases"), volumeTransactions);
        }
        
        assert transactions == volumeTransactions;   
        
    }
    public static void main(String[] args) {       
        Test test = new Test();
        //test.verifyAccounts();
        //System.out.println("Accounts verified!");
        //test.verifyTransactions();
        //System.out.println("Transactions verfied!");
        
        test.process("Benjamin Stone");
    }
}