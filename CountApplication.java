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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class CountApplication extends Configured implements Tool{
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CountApplication(), args);
        System.exit(res);
    }
    
    
    @Override
    public int run(String[] strings) throws Exception { //To change body of generated methods, choose Tools | Templates.
          long startTime = System.currentTimeMillis();
        if(strings.length != 2) {
            System.out.println("usage: [input] [output]");
            System.exit(-1);
        }
        Job job = Job.getInstance(new Configuration());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        job.setJarByClass(CountApplication.class);
        job.submit();
        long endTime = System.currentTimeMillis();
        System.out.println("Took "+(endTime - startTime) + " ns"); 
        return 0;
    }
    
}
