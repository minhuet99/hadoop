package com.msb.dmp;

import com.msb.dmp.salary.SalaryMapper;
import com.msb.dmp.salary.SalaryReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;


public class Main {
    public static void main(String[] args) throws Exception {
        String innputPath = "D:\\msb\\code\\hadoop\\mapreduce\\msb_mapreduce_oracle\\data\\staff.txt";
        String outputPath = "D:\\msb\\code\\hadoop\\mapreduce\\msb_mapreduce_oracle\\data\\result.txt";


        JobConf jobConf = new JobConf();
        jobConf.setJarByClass(Main.class);
        jobConf.setJobName("cal salary");
        FileInputFormat.addInputPath(jobConf, new Path(innputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(DoubleWritable.class);
        Job job = new Job(jobConf);
        job.setMapperClass(SalaryMapper.class);
        job.setReducerClass(SalaryReducer.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}