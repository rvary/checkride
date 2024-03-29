package com.confluent.checkride;
import java.io.*;
import java.net.http.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.net.URI;
import java.util.*;

import org.json.*;

import com.mongodb.lang.Nullable;

public class Data 
{
    private String APIKEY = "";
    private final String HOST = "twelve-data1.p.rapidapi.com";
    private final String USERDIR = System.getProperty("user.dir");
    private int APITHROTTLING = 0, RETURNS = 0, APICOUNT = 0;
    private boolean LOG = false;
    private FileWriter fw;
    
    public TreeMap<String,Stock> nasdaq, midLargeCap;
    public Stack<String> stocks, copy;
    public ArrayList<String> names;

    public Data(){}
    public Data(boolean update) {
        nasdaq = new TreeMap<>();
        midLargeCap = new TreeMap<>();
        stocks = new Stack<>();
        names = new ArrayList<>();
        String data;
        try{
            BufferedReader br = new BufferedReader(new FileReader(USERDIR + "/data/names.csv"));
            while((data = br.readLine()) != null){
                names.add(data);
                data = br.readLine();
            }
            br.close();
            if(LOG){
                fw = new FileWriter(new File("log.txt"), false);
            }
            if(update){
                br = new BufferedReader(new FileReader(USERDIR + "keys"));
                APIKEY = br.readLine();
                br.close();
                writeStocksToDisk("stocks.json", getStocks(null));
                writeStocksToDisk("midLargeCap.json", getMidtoLargeCapStocks());
            }
            {
                nasdaq = dataRead("stocks.json");
                midLargeCap = dataRead("midLargeCap.json");
            }
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
    }
    public ArrayList<String> getNames(){
        String data;
        names = new ArrayList<String>();
        try{
            BufferedReader br = new BufferedReader(new FileReader(USERDIR + "/data/names.csv"));
            while((data = br.readLine()) != null){
                names.add(data);
                data = br.readLine();
            }
            br.close();
        }catch(IOException e){
            e.printStackTrace();
        }
        return names;
    }
    public void readStocks(String file){
        stocks = new Stack<String>();
        try{
            BufferedReader br = new BufferedReader(new FileReader(USERDIR + "/keys"));
            APIKEY = br.readLine();
            String[] stocksToRead = Files.readAllLines(Paths.get(USERDIR+"/data/"+file)).toArray(new String[0]);
            for(String stock : stocksToRead){
                stocks.push(stock);
            }
            br.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }
    private TreeMap<String,Stock> getStocks(@Nullable Stack<String> stack) throws InterruptedException, IOException{
        HttpRequest request;
        JSONObject response, stock;
        JSONArray data;
        String sym;
        Stack<String> copy = new Stack<String>();
        if(stack != null){
            TreeMap<String,Stock> nasdaqStocks = new TreeMap<>();
            while(!stack.empty()){
                Thread.sleep(1000);
                sym = stack.pop();
                System.out.println("GET " + sym);
                copy.push(sym);
                request = HttpRequest.newBuilder()
                    .uri(URI.create("https://twelve-data1.p.rapidapi.com/symbol_search?symbol="+sym+"&outputsize=1"))
                    .header("X-RapidAPI-Key", APIKEY)
                    .header("X-RapidAPI-Host", HOST)
                    .method("GET", HttpRequest.BodyPublishers.noBody())
                    .build();
                
                response = new JSONObject(HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString()).body());
                data = new JSONArray(response.getJSONArray("data"));
                stock = (JSONObject)data.get(0);
                nasdaqStocks.put(sym, new Stock(sym, stock.getString("instrument_name"), stock.getString("instrument_type")));
            }
            while(!copy.empty()) stack.push(copy.pop());     
            return nasdaqStocks;
        }
        else{
            request = HttpRequest.newBuilder()
                .uri(URI.create("https://"+HOST+"/stocks?country=US&exchange=NASDAQ&format=json"))
                .header("X-RapidAPI-Key", APIKEY)
                .header("X-RapidAPI-Host", HOST)
                .method("GET", HttpRequest.BodyPublishers.noBody())
                .build();
            if(LOG){
                fw.write("API CALL: https://"+HOST+"/stocks?country=US&exchange=NASDAQ&format=json\n");
                fw.flush();
            }
            response = new JSONObject(HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString()).body());
            data = new JSONArray(response.getJSONArray("data"));;
                
            for(int j = 0; j < data.length(); j++){
                stock = data.getJSONObject(j);
                stocks.push(stock.getString("symbol"));
                nasdaq.put(stock.getString("symbol"), (new Stock(stock.getString("symbol"), stock.getString("name"), stock.getString("type"))));
            }
            getStockPrices();
            return nasdaq;
        }
    }
    private void getStockPrices() throws IOException, InterruptedException {
        String stock;
        HttpResponse<String> response = null;
        JSONObject raw = null;
        while(!stocks.isEmpty()){
            stock = stocks.pop();
            System.out.println("GET " + stock + " PRICE");
            try{
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://"+HOST+"/price?symbol="+stock+"&format=json&outputsize=30"))
                    .header("X-RapidAPI-Key", APIKEY)
                    .header("X-RapidAPI-Host", HOST)
                    .method("GET", HttpRequest.BodyPublishers.noBody())
                    .build();
                    if(LOG){
                        System.out.println("API CALL " + ++APICOUNT);
                        fw.write("API CALL " + APICOUNT + ": https://"+HOST+"/price?symbol="+stock+"&format=json&outputsize=30\n");
                        fw.flush();
                    }
                    Thread.sleep(865);
                    response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
                    raw = new JSONObject(response.body());
                    nasdaq.get(stock).setPrice(raw.getDouble("price"));
            }catch(Exception e){
                if(e instanceof JSONException && response.statusCode() == 429){
                    if(LOG){
                        System.out.println("API THROTTLE " + ++APITHROTTLING);
                        fw.write("API throttle event: " + APITHROTTLING + " - Sleeping 30s. \n");
                        fw.flush();
                    }
                    stocks.push(stock);
                    Thread.sleep(30000);
                    getStockPrices();
                }
                else if(e instanceof JSONException && raw.getInt("code") == 400){
                    System.out.println("BAD API CALL - REMOVING STOCK");
                    nasdaq.remove(stock);
                    getStockPrices();
                }
                else{
                    throw e;
                }
            }
        }
        if(LOG){
            System.out.println("RETURN" + ++RETURNS);
            fw.write("Return event: " + RETURNS + "\n");
            fw.flush();
        }
    }
    private TreeMap<String,Stock> getMidtoLargeCapStocks() throws IOException {
        String line;
        String[] data;
        TreeMap<String,Stock> stocks = new TreeMap<>();
        BufferedReader br = new BufferedReader(new FileReader(
            System.getProperty("user.dir") + "/data/nasdaq_mid_to_large_cap.csv"));

        while((line = br.readLine()) != null){
            data = line.split(",");
            if(nasdaq.get(data[0]) != null){
                stocks.put(data[0], nasdaq.get(data[0]));
            }
        }
        br.close();

        return stocks;
    }
    private void writeStocksToDisk(String file, TreeMap<String,Stock> data) throws IOException{
        JSONArray ja = new JSONArray();
        JSONObject jo;
        Stock stock;
        for(Map.Entry<String,Stock> entry : data.entrySet()){
            jo = new JSONObject();
            stock = entry.getValue();
            try{
                jo.put("symbol", stock.symbol);
                System.out.println(stock.symbol);
            }catch(NullPointerException e){
                e.printStackTrace();
            }
            jo.put("companyName", stock.companyName);
            jo.put("price", stock.price);
            jo.put("type", stock.type);
            ja.put(jo);
        }
        fw = new FileWriter(USERDIR+"/data/" + file);
        fw.write(ja.toString());
        fw.close();
    } 
    private TreeMap<String,Stock> dataRead(String file) throws IOException{
        JSONArray ja = new JSONArray(new JSONTokener(new BufferedReader(new FileReader(System.getProperty("user.dir")+"/data/"+file))));
        JSONObject jo = new JSONObject();
        TreeMap<String,Stock> stocks = new TreeMap<>();   
        for(int j = 0; j < ja.length(); j++){
            jo = ja.getJSONObject(j);
            //System.out.println(jo.getString("symbol")+ jo.getString("companyName") + jo.getDouble("price")+ jo.getString("type"));
            stocks.put(jo.getString("symbol"), 
                (new Stock(jo.getString("symbol"), jo.getString("companyName"), 
                    jo.getDouble("price"), jo.getString("type"))));
        }
        return stocks;   
    }
    public JSONArray readJSON(String file) throws IOException{
        JSONArray ja = new JSONArray(new JSONTokener(new BufferedReader(new FileReader(System.getProperty("user.dir")+"/data/"+file))));
        return ja;
    }
    public void read(String read, String write) throws IOException{    
        String[] stocks = Files.readAllLines(Paths.get(USERDIR+"/data/"+read)).toArray(new String[0]);
        TreeMap<String,Stock> container = new TreeMap<String,Stock>();
        for(int j = 0; j < stocks.length; j++){
            container.put(stocks[j], nasdaq.get(stocks[j]));
        }
        writeStocksToDisk(write, container);
    }

    public static void main( String[] args ) throws IOException
    {
        Data data = new Data();
        try{
            data.nasdaq = data.getStocks(data.stocks);
            data.getStockPrices();
            data.writeStocksToDisk("nasdaq.json", data.nasdaq);
        }catch(InterruptedException e){
            e.printStackTrace();
        }
    }
}