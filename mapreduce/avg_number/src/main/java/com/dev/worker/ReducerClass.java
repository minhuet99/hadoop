package com.dev.worker;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<IntWritable, IntWritable, Text, DoubleWritable> {

    private DoubleWritable avg = new DoubleWritable();
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int num = 0;
        for (IntWritable value : values) {
            sum += value.get();
            num++;
        }
        avg.set(sum/num);
        context.write(new Text("avg"), avg);
    }
}
