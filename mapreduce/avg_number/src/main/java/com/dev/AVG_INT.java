package com.dev;

import com.dev.worker.MapperClass;
import com.dev.worker.ReducerClass;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class AVG_INT extends Configured implements Tool {
    public int run(String[] arg0) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(AVG_INT.class);
        job.setJobName("avg integer");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Double.class);

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        FileInputFormat.setInputPaths(job, new Path("/tmp/avg/in"));
        FileOutputFormat.setOutputPath(job, new Path("/tmp/avg/out"));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
