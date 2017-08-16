package hadoop.mapred;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by xtutran on 13/5/17.
 */
public class WordCount extends Configured implements Tool {

    public static class MapClass extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();
        private final IntWritable one = new IntWritable(1);

        public void map(LongWritable key, // Offset into the file
                        Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {
            // Get the value as a String
            String text = value.toString().toLowerCase();

            // Replace all non-characters
            text = text.replaceAll("'", "");
            text = text.replaceAll("[^a-zA-Z]", " ");

            // Iterate over all of the words in the string
            StringTokenizer st = new StringTokenizer(text);
            while (st.hasMoreTokens()) {
                // Get the next token and set it as the text for our "word" variable
                word.set(st.nextToken());

                // Output this word as the key and 1 as the value
                output.collect(word, one);
            }
        }
    }

    //map
    // 1. english a b french => (english, 1), (a, 1) (b, 1) (french, 1) datanode1
    // 2. english c d dutch => (english, 1), (c, 1) (d, 1) (dutch 1) datanode2

    // datanode1 is more efficient than datanode2 / or against
    // datanode1 is good to copy, copy (english, 1) from datanode2 to datanode1

    // go to one reducer (in datanode1)
    // english: key
    // values (1, 1)
    // count += 1
    //count += 1
    // (english, 2)
    // write (english, 2) to hdfs ...

    //copy, sort and shuffle
    //[(english, 1) (english, 1)], [(a,1)], [(b,1)] .....


    public static class Reduce extends MapReduceBase
            implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {
            // Iterate over all of the values (counts of occurrences of this word)
            int count = 0;
            while (values.hasNext()) {
                // Add the value to our count
                count += values.next().get();
            }

            // Output the word with its count (wrapped in an IntWritable)
            output.collect(key, new IntWritable(count));
        }
    }


    public int run(String[] args) throws Exception {
        // Create a configuration
        Configuration conf = getConf();

        // Create a job from the default configuration that will use the WordCount class
        JobConf job = new JobConf(conf, WordCount.class);

        // Define our input path as the first command line argument and our output path as the second
        Path in = new Path("data/mapred/input");
        Path out = new Path("data/mapred/output");

        // Create File Input/Output formats for these paths (in the job)
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        // Configure the job: name, mapper, reducer, and combiner
        job.setJobName("WordCount");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Reduce.class);

        // Configure the output
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Run the job
        JobClient.runJob(job);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        // Start the WordCount MapReduce application
        int res = ToolRunner.run(new Configuration(),
                new WordCount(),
                args);
        System.exit(res);
    }
}
