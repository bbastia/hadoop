package org.upgrad.mapreduce;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

@Slf4j
public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
		@Override

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	log.info("Mapper invoked ..");

			char c = value.toString().charAt(0);

			String st [] = value.toString().split("\\s+");

        for(String st1 :  st) {

            context.write(new Text(st1.replaceAll("[^a-zA-Z]","").toLowerCase()), new IntWritable(1));
        }

	}
}
