package com.msb.dmp.salary;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SalaryMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    private static final String COMMA = ",";
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
        String[] cell = value.toString().trim().split(COMMA);
        String staffCode = cell[0];
        MapWritable staffInfo = new MapWritable();
        IntWritable numWorkdays = new IntWritable(Integer.parseInt(cell[1]));
        DoubleWritable salary = new DoubleWritable(Double.parseDouble(cell[2]));
        staffInfo.put(new Text("numWorkdays"), numWorkdays);
        staffInfo.put(new Text("salary"), salary);
        context.write(new Text(staffCode), staffInfo);
    }
}
