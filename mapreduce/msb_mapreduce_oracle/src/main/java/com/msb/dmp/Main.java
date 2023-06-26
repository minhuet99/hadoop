package com.msb.dmp;

import com.msb.dmp.salary.SalaryMapper;
import com.msb.dmp.salary.SalaryReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Main extends Configured {
    public void main(String[] args) throws IOException {
        String innputPath = "D:\\msb\\code\\hadoop\\mapreduce\\msb_mapreduce_oracle\\data\\staff.txt";
        String outputPath = "D:\\msb\\code\\hadoop\\mapreduce\\msb_mapreduce_oracle\\data\\result.txt";




        Job job = new Job(getConf());
        job.setJarByClass(Main.class);
        job.setJobName("cal salary");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Double.class);

        job.setMapperClass(SalaryMapper.class);
        job.setReducerClass(SalaryReducer.class);

        FileInputFormat.setInputPaths(job, new Path("/tmp/avg/in"));
        FileOutputFormat.setOutputPath(job, new Path("/tmp/avg/out"));
    }
}