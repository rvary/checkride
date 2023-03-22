package com.confluent.checkride;

public class Position {
    private Stock stock;
    private int shares;
    private Double costBasis;
    private Double gain;
    private Double positionValue;
    public Position(){}
    public Position(Stock equity, int numShares){
        stock = equity;
        shares = numShares;
        positionValue = stock.getPrice()*shares;
        gain = 0.0;
        costBasis = stock.getPrice();
    }
    public void setStock(Stock stock){
        this.stock = stock;
    }
    public Stock getStock(){
        return stock;
    }
    public void setShares(final int shares){
        this.shares = shares;
    }
    public int getShares(){
        return shares;
    }
    public void setCostBasis(Double basis){
        costBasis = basis;
    }
    public Double getCostBasis(){
        return costBasis;
    }
    public void setGain(Double gain){
        this.gain = gain;
    }
    public Double getGain(){
        return gain;
    }
    public void setPositionValue(Double value){
        positionValue = value;
    }
    public Double getPositionValue(){
        return positionValue;
    }
    public boolean canSell(int numShares){
        return (numShares <= shares) ? true : false;   
    }
    public void sell(int numShares){
        shares-=numShares;
    }
    public void buy(int numShares){
        shares += numShares;
    }
}