package com.msb.dmp.salary;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SalaryReducer extends Reducer<Text, MapWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Reducer<Text, MapWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        double salary = 0;
        for (MapWritable staffInfo : values) {
            salary += ((IntWritable) staffInfo.get(new Text("numWorkdays"))).get() * ((DoubleWritable) staffInfo.get(new Text("salary"))).get();
        }
        context.write(key, new DoubleWritable(salary));
    }
}
