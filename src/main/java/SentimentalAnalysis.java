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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SentimentalAnalysis {
    public static class SentenceSplit extends Mapper<Object, Text, Text, IntWritable> {
        public Map<String, String> externalDic = new HashMap<String, String>();

        @Override
        public void setup(Context context) throws IOException{
            Configuration configuration = context.getConfiguration();
            String dicName = configuration.get("Dictionary", "");

            BufferedReader br = new BufferedReader(new FileReader(dicName));
            String line = br.readLine();
            while (line != null) {
                String[] words = line.split("\t");
                externalDic.put(words[0].toLowerCase(), words[1]);
                line = br.readLine();
            }
            br.close();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String filePath = ((FileSplit)context.getInputSplit()).getPath().getName();
            String sentence = value.toString().trim();
            String[] words = sentence.split("\\s+");
            for (String word: words) {
                if (externalDic.containsKey(word.trim().toLowerCase())) {   //word.trim()
                    context.write(new Text(filePath + "\t" + externalDic.get(word.toLowerCase())), new IntWritable(1));
                }
            }

        }
    }

    public static class SentimentCount extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("Dictionary", args[2]);

        Job job = new Job(configuration);
        job.setJarByClass(SentimentalAnalysis.class);
        job.setMapperClass(SentenceSplit.class);
        job.setReducerClass(SentimentCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
