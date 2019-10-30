// Реализуйте подсчет среднего рейтинга продуктов. Результат сохранить в HDFS в файле "avg_rating.csv".
// Формат каждой записи: ProdId, Rating

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

// Start hadoop:
// /usr/local/Cellar/hadoop/3.2.1/sbin/start-dfs.sh
// /usr/local/Cellar/hadoop/3.2.1/sbin/start-yarn.sh
// mapred --daemon start historyserver

// To run:
// hadoop dfs -rm -r -f /filter_prod_name_avg_rating
// yarn jar ./out/artifacts/HW1/HW1.jar FilterProdNameAvgRating -D mapred.textoutputformat.separator="," -D mapreduce.job.reduces=2 canon /prodname_avg_rating /filter_prod_name_avg_rating

public class FilterProdNameAvgRating  extends Configured implements Tool {

    public static class FilterProdNameAvgRatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String search;
        private final Text id = new Text();
        private final Text nameRating = new Text();

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            search = conf.get("search").toLowerCase();
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",", 3);
            if (data.length >= 3 && data[1].toLowerCase().contains(search)) {
                id.set(data[0]);
                nameRating.set(data[1] + ',' + data[2]);
                context.write(id, nameRating);
            }
        }
    }


    public int run(String[] args) throws Exception {
        String search = args[0];
        final Path inputPath = new Path(args[1]);
        final Path outputPath = new Path(args[2]);

        Configuration conf = getConf();
        conf.set("search", search);

        // Create a new MapReduce job
        Job job = Job.getInstance(conf, "FilterProdNameAvgRating");
        //  Set the Jar by finding where a given class came from
        job.setJarByClass(FilterProdNameAvgRating.class);
        // Set the Mapper for the job
        job.setMapperClass(FilterProdNameAvgRatingMapper.class);
        // Set the key class for the job output data
        job.setOutputKeyClass(Text.class);
        // Set the value class for job outputs
        job.setOutputValueClass(Text.class);

        // Add a Path to the list of inputs for the map-reduce job
        FileInputFormat.addInputPath(job, inputPath);
        // Set the Path of the output directory for the map-reduce job.
        FileOutputFormat.setOutputPath(job, outputPath);

        // Submit the job to the cluster and wait for it to finish
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        System.out.println("Start FilterProdNameAvgRating");

        /*
          Runs the given Tool by Tool.run(String[]), after
          parsing with the given generic arguments. Uses the given
          Configuration, or builds one if null.
         */
        System.exit(ToolRunner.run(conf, new FilterProdNameAvgRating(), args));
    }
}
