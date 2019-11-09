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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context output) throws IOException, InterruptedException{
        int voteCount = 0;
        for(IntWritable value: values){
            voteCount += value.get();
        }
        output.write(key, new IntWritable(voteCount));
    }
    
}
