package com.confluent.checkride;

import java.util.TreeMap;
import java.util.Map.Entry;
import org.bson.types.ObjectId;;

public class Account {
    private ObjectId _id;
    private String holder;
    private Double cash;
    private Double accountValue;
    private TreeMap<String,Position> positions;
    public Account(){}
    public Account(String name){
        holder = name;
        accountValue = 0.0;
        cash = 0.0;
        positions = new TreeMap<String,Position>();
    }
    public void setId(ObjectId id){
        this._id = id;
    }
    public ObjectId getId(){
        return _id;
    }
    public void setHolder(String holder){
        this.holder = holder;
    }
    public String getHolder(){
        return holder;
    }
    public void setCash(Double deposit){
        cash = deposit;
    }
    public Double getCash(){
        return cash;
    }
    public void setAccountValue(Double value){
        accountValue = value;
    }
    public Double getAccountValue(){
        return accountValue;
    }
    public void setPositions(TreeMap<String,Position> positions){
        this.positions = positions;
    }
    public TreeMap<String,Position> getPositions(){
        return positions;
    }
    public void credit(Double credit){
        cash+=credit;
    }
    public void debit(Double withdrawal){
        cash-=withdrawal;
    }
    public void addPosition(String stock, Position position){
        positions.put(stock, position);
    }
    public void getAccountDetails(){
        System.out.printf("Acct: %s\n", getHolder());
        System.out.printf("Acct Value: %f\n", getAccountValue());
        for(Entry<String, Position> position : positions.entrySet()){
            Position p = position.getValue();
            Stock stock = position.getValue().getStock();
            System.out.printf("Stock: %s, Shares: %d, Share Price: $%f, Position Value: $%f", 
                stock.getSymbol(), p.getShares(), stock.getPrice(), p.getValue());
        } 
    }
}