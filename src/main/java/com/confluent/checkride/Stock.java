package com.confluent.checkride;

public class Stock {
    protected String symbol;
    protected String companyName;
    protected Double price;
    protected String type;
    public Stock(){}
    public Stock(String s, String cName, String t){
        symbol = s;
        companyName = cName;
        type = t;
    }
    public Stock(String s, String cName, Double p, String t){
        symbol = s;
        companyName = cName;
        price = p;
        type = t;
    }
    public void setSymbol(String symbol){
        this.symbol = symbol;
    }
    public String getSymbol(){
        return symbol;
    }
    public void setCompanyName(String name){
        companyName = name;
    }
    public String getCompanyName(){
        return companyName;
    }
    public void setPrice(Double p){
        price = p;
    }
    public Double getPrice(){
        return price;
    }
    public void setType(String type){
        this.type = type;
    }
    public String getType(){
        return type;
    }
}