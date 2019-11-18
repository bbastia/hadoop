package org.upgrad.dataType;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.mapreduce.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

@Slf4j
public class WordCountMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
    private TextPair wordPair = new TextPair();
    private IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int neighbors = context.getConfiguration().getInt("neighbors", 2);
        String[] tokens = value.toString().split("\\s+");
        log.info("tokens={}", tokens);
        if (tokens.length > 1) {
            for (int i = 0; i < tokens.length; i++) {
                wordPair.setFirst(new Text(tokens[i]));
                log.info("firstToken={}", tokens[i]);
                int start = (i - neighbors < 0) ? 0 : i - neighbors;
                int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
                for (int j = start; j <= end; j++) {
                    if (j == i) continue;
                    wordPair.setSecond(new Text(tokens[j]));
                    log.info("SecondToken={}", tokens[j]);
                    context.write(wordPair, ONE);
                }
            }
        }
    }
}

