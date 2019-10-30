// Реализуйте подсчет среднего рейтинга продуктов. Результат сохранить в HDFS в файле "avg_rating.csv".
// Формат каждой записи: ProdId, Rating

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

// Start hadoop:
// /usr/local/Cellar/hadoop/3.2.1/sbin/start-dfs.sh
// /usr/local/Cellar/hadoop/3.2.1/sbin/start-yarn.sh
// mapred --daemon start historyserver

// To run:
// hadoop dfs -rm -r -f /avg_rating
// yarn jar ./out/artifacts/HW1/HW1.jar AvgRating -D mapred.textoutputformat.separator="," -D mapreduce.job.reduces=2 /reviews_Electronics_5.json /avg_rating

public class AvgRating  extends Configured implements Tool {

    public static class VoteMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final JSONParser jsonParser = new JSONParser();
        private final Text product = new Text();
        private final DoubleWritable vote = new DoubleWritable();

        /**
         * Called once for each key/value pair in the input split.
         *
         * @param key       a document offset
         * @param value     a text string
         * @param context   a application context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                JSONObject review = (JSONObject) jsonParser.parse(value.toString());
                // Set the word to serializable class
                product.set((String) review.get("asin"));
                vote.set((double) review.get("overall"));
                // Emmit the key-value pair: (word, 1)
                context.write(product, vote);
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
    }


    public static class VoteAvgReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            long count = 0;
            for (DoubleWritable vote : values) {
                sum += vote.get();
                ++count;
            }
            result.set(sum / count);
            context.write(key, result);
        }
    }

    public int run(String[] args) throws Exception {
        // Create a new MapReduce job
        Job job = Job.getInstance(getConf(), "AvgRating");
        //  Set the Jar by finding where a given class came from
        job.setJarByClass(AvgRating.class);
        // Set the Mapper for the job
        job.setMapperClass(VoteMapper.class);
        // Set the Reducer for the job
        job.setReducerClass(VoteAvgReducer.class);
        // Set the key class for the job output data
        job.setOutputKeyClass(Text.class);
        // Set the value class for job outputs
        job.setOutputValueClass(DoubleWritable.class);

        // Add a Path to the list of inputs for the map-reduce job
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // Set the Path of the output directory for the map-reduce job.
        final Path output_dir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, output_dir);

        // Submit the job to the cluster and wait for it to finish
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        System.out.println("Start AvgRating");

        /*
          Runs the given Tool by Tool.run(String[]), after
          parsing with the given generic arguments. Uses the given
          Configuration, or builds one if null.
         */
        System.exit(ToolRunner.run(conf, new AvgRating(), args));
    }
}
