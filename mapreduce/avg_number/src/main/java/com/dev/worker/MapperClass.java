package com.dev.worker;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapperClass extends Mapper<Object, Text, IntWritable, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            context.write(ONE, new IntWritable(Integer.parseInt(token)));
        }
    }
}
