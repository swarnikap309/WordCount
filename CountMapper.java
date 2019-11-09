/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cps534.count;

/**
 *
 * @author aditya
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class CountMapper extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
     HashMap<String, Integer> map;
     ArrayList<String> wordArray = new ArrayList<String>();
   
    @Override
    public void setup(Context output) {
         map = new HashMap<String, Integer>();
    }
   
    @Override
    public void map(Object key, Text value, Context output) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
     
       //VERSION 1
       // output.write(new Text(words[0]), one)
       
       
       /*
       Version 2
       
       
       String[] words = value.toString().split(" ");
     
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        ArrayList<String> wordArray = new ArrayList<String>();
       for(String word : words){
           int count = 0;
           if(map.containsKey(word)){
               count = map.get(word);
           } else{
               wordArray.add(word);
           }
           map.put(word, ++count);
       }
       String dumStr = "";
       for(int i = 0; i < map.size(); i++){
                dumStr = wordArray.get(i);
                output.write(new Text(dumStr), new IntWritable(map.get(dumStr)));  
       }
       */
     
       for(String word : words){
           int count = 0;
           if(map.containsKey(word)){
               count = map.get(word);
           } else{
               wordArray.add(word);
           }
           map.put(word, ++count);
       }
       
    }
   
    @Override
    public void cleanup(Context output) throws IOException, InterruptedException {
       String dumStr = "";
       for(int i = 0; i < map.size(); i++){
                dumStr = wordArray.get(i);
                output.write(new Text(dumStr), new IntWritable(map.get(dumStr)));  
       }
    }
}