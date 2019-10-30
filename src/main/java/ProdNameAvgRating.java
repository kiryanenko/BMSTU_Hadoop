// Напишите программу, которая каждому ProdId из "avg_rating.csv" ставит в соответстие названием продукта.
// Результат сохранить в HDFS в файле "prodname_avg_rating.csv": ProdId,Name,Rating

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Start hadoop:
// /usr/local/Cellar/hadoop/3.2.1/sbin/start-dfs.sh
// /usr/local/Cellar/hadoop/3.2.1/sbin/start-yarn.sh
// mapred --daemon start historyserver

// To run:
// hadoop dfs -rm -r -f  /prodname_avg_rating
// yarn jar ./out/artifacts/HW1/HW1.jar ProdNameAvgRating -D mapred.textoutputformat.separator="," -D mapreduce.job.reduces=2 /avg_rating/ /meta_Electronics.json /prodname_avg_rating

public class ProdNameAvgRating  extends Configured implements Tool {

    public static class JoinProdNameAvgRating implements Writable {
        static final byte PROD_NAME_TYPE = 'n';
        static final byte AVG_RATING_TYPE = 'r';

        private byte type;
        private String prodName;
        private double avgRating;

        void setProdName(String name) {
            type = PROD_NAME_TYPE;
            prodName = name;
        }

        void setAvgRating(double rating) {
            type = AVG_RATING_TYPE;
            avgRating = rating;
        }

        byte getType() {
            return type;
        }

        String getProdName() {
            return prodName;
        }

        double getAvgRating() {
            return avgRating;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeByte(type);
            if (type == PROD_NAME_TYPE) {
                dataOutput.writeChars(prodName);
            } else {
                dataOutput.writeDouble(avgRating);
            }
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            type = dataInput.readByte();
            if (type == PROD_NAME_TYPE) {
                prodName = dataInput.readLine();
            } else {
                avgRating = dataInput.readDouble();
            }
        }
    }

    public static class AvgRatingMapper extends Mapper<LongWritable, Text, Text, JoinProdNameAvgRating> {

        private final Text id = new Text();
        private final JoinProdNameAvgRating join = new JoinProdNameAvgRating();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] data = value.toString().split(",", 2);

            id.set(data[0]);

            double rating = 0;
            if (data.length == 2) {
                try {
                    rating = Double.parseDouble(data[1]);
                } catch (NumberFormatException ignored) {}
            }
            join.setAvgRating(rating);

            context.write(id, join);
        }
    }

    public static class ProdNameMapper extends Mapper<LongWritable, Text, Text, JoinProdNameAvgRating> {

        private final JSONParser jsonParser = new JSONParser();
        private final Text id = new Text();
        private final JoinProdNameAvgRating join = new JoinProdNameAvgRating();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String buf = value.toString();
                buf = buf.replace('\'', '"');
                JSONObject json = (JSONObject) jsonParser.parse(buf);
                String asin = (String) json.get("asin");
                String title = (String) json.get("title");
                if (asin != null && title != null) {
                    id.set((String) json.get("asin"));
                    join.setProdName((String) json.get("title"));
                    context.write(id, join);
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * Combiner/Reducer class
     *
     */
    public static class ProdNameAvgReducer extends Reducer<Text, JoinProdNameAvgRating, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<JoinProdNameAvgRating> values, Context context)
                throws IOException, InterruptedException {
            String name = null;
            Double rating = null;

            for (JoinProdNameAvgRating join : values) {
                if (join.getType() == JoinProdNameAvgRating.PROD_NAME_TYPE) {
                    name = join.getProdName();
                } else {
                    rating = join.getAvgRating();
                }
            }

            if (name != null && rating != null) {
                result.set(name + ',' + rating);
                context.write(key, result);
            }
        }
    }

    public int run(String[] args) throws Exception {
        // Create a new MapReduce job
        Job job = Job.getInstance(getConf(), "ProdNameAvgRating");
        //  Set the Jar by finding where a given class came from
        job.setJarByClass(ProdNameAvgRating.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinProdNameAvgRating.class);
        // Set the Reducer for the job
        job.setReducerClass(ProdNameAvgReducer.class);
        // Set the key class for the job output data
        job.setOutputKeyClass(Text.class);
        // Set the value class for job outputs
        job.setOutputValueClass(Text.class);

        final Path avgRatingPath = new Path(args[0]);
        final Path prodNamePath = new Path(args[1]);
        final Path outputPath = new Path(args[2]);

        MultipleInputs.addInputPath(job, avgRatingPath, TextInputFormat.class, AvgRatingMapper.class);
        MultipleInputs.addInputPath(job, prodNamePath, TextInputFormat.class, ProdNameMapper.class);

        // Set the Path of the output directory for the map-reduce job.
        FileOutputFormat.setOutputPath(job, outputPath);

        // Submit the job to the cluster and wait for it to finish
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        System.out.println("Start ProdNameAvgRating");

        /*
          Runs the given Tool by Tool.run(String[]), after
          parsing with the given generic arguments. Uses the given
          Configuration, or builds one if null.
         */
        System.exit(ToolRunner.run(conf, new ProdNameAvgRating(), args));
    }
}
