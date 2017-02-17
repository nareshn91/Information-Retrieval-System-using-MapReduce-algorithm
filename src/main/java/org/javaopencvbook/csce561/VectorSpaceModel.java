/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.javaopencvbook.csce561;

/**
 *
 * @author Akash
 */
 


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 *
 * @author akash
 */
public class VectorSpaceModel  
{

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
//    public int run(String[] args) throws Exception
//    {
//        if(args.length<2)
//        {
//            System.out.println("plz give valid input and output");
//            return -1;
//        }
//        JobConf job = new JobConf(Main.class);
//        Job j = new Job();
//        j.
//        
//        
//    
//    FileInputFormat.setInputPaths(job, new Path(args[0]));
//    FileOutputFormat.setOutputPath(job, new Path(args[1]));
//    job.setMapperClass(InMapper.class);
//    
//    
//    job.setMapOutputKeyClass(Text.class);
//    job.setMapOutputValueClass(IntWritable.class);
//    
//    job.setOutputKeyClass(Text.class);
//    job.setOutputValueClass(IntWritable.class);
//    
//    JobClient.runJob(job);
//        
//    return 0;
//    
//    
//    }
//    public static void main(String args[]) throws Exception
//    {
//        int exitCode = ToolRunner.run(new Main(), args);
//        System.exit(exitCode);
//    }

    public static void main(String[] args) throws Exception 
 {
     
    Configuration conf = new Configuration();
        
        
//    String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = new Job(conf, "wordcount");
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(InMapper.class);
    job.setReducerClass(InReducer.class);
    job.setJarByClass(VectorSpaceModel.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[1]));
    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
    job.waitForCompletion(true);
 
 }
}

