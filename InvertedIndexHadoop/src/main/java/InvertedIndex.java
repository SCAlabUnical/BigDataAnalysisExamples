import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndex {

    static class Map extends Mapper<Object, Text, Text, IntWritable> {

        private final Text keyInfo = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Extract the filename from the current input split
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                // Remove punctuation, apply lemmatization and stemming if required
                String word = itr.nextToken().toLowerCase().replaceAll("\\p{Punct}", "");
                keyInfo.set(word + ":" + filename);
                context.write(keyInfo, one);
            }
        }
    }

    static class Combine extends Reducer<Text, IntWritable, Text, Text> {
        private final Text sumInfo = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            // Sum all the occurrences of a word in the document
            for (IntWritable value : values)
                sum += value.get();
            int splitIndex = key.toString().indexOf(":");
            sumInfo.set(key.toString().substring(splitIndex + 1) + ":" + sum);
            key.set(key.toString().substring(0, splitIndex));
            context.write(key, sumInfo);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        private final Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder fileList = new StringBuilder();
            for (Text value : values)
                fileList.append(value.toString()).append(";");

            result.set(fileList.toString());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);
        // Set the output class of key and value for the mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // Set the output class of key and value for the reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // Specify input and output paths
        FileInputFormat.addInputPaths(job, "file1.txt,file2.txt");
        FileOutputFormat.setOutputPath(job, new Path("out"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
