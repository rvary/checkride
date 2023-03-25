package com.confluent.checkride;

import java.util.TreeMap;
import java.util.Map.Entry;
import org.bson.types.ObjectId;;

public class Account {
    private ObjectId _id;
    private String holder;
    private Double cash;
    private Double accountValue;
    private Double accountGain;
    private TreeMap<String,Position> positions;
    public Account(){}
    public Account(String name){
        holder = name;
        cash = 0.0;
        accountValue = 0.0;
        accountGain = 0.0;
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
    public void setAccountGain(Double gain){
        accountGain = gain;
    }
    public Double getAccountGain(){
        return accountGain;
    }
    public void setPositions(TreeMap<String,Position> positions){
        this.positions = positions;
    }
    public TreeMap<String,Position> getPositions(){
        return positions;
    }
    public void addPosition(String stock, Position position){
        positions.put(stock, position);
    }
}