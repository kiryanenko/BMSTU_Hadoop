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

// Запуск hadoop:
// /usr/local/Cellar/hadoop/3.2.1/sbin/start-dfs.sh
// /usr/local/Cellar/hadoop/3.2.1/sbin/start-yarn.sh
// mapred --daemon start historyserver

// Команды для старта:
// hadoop dfs -rm -r -f /avg_rating
// yarn jar ./out/artifacts/HW1/HW1.jar AvgRating -D mapred.textoutputformat.separator="," -D mapreduce.job.reduces=2 /reviews_Electronics_5.json /avg_rating

public class AvgRating  extends Configured implements Tool {

    // Класс отображения, на вход подаются записи отзывом о продукте в формате JSON
    // На выход отдает id продукта и оценку
    public static class VoteMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final JSONParser jsonParser = new JSONParser();
        private final Text product = new Text();
        private final DoubleWritable vote = new DoubleWritable();

        // Функция отображения, которая вызывается для каждой записи в файле
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                // Распарсиваю JSON c отзывом
                JSONObject review = (JSONObject) jsonParser.parse(value.toString());
                // Достаю id продукта и оценку
                product.set((String) review.get("asin"));
                vote.set((double) review.get("overall"));
                // Отдаю их на выход
                context.write(product, vote);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }


    // Класс свертки, на вход как ключ подается id продукта и массив оценок, соответствующих этому продукту
    // На выход отдает id продукта и среднюю оценку
    public static class VoteAvgReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        // Функция свертки
        // На вход получает id продукта как ключ и оценки как значения
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            // Подсчитываю среднюю оценку
            double sum = 0;
            long count = 0;
            for (DoubleWritable vote : values) {
                sum += vote.get();
                ++count;
            }
            result.set(sum / count);
            // Отдаю результат
            context.write(key, result);
        }
    }

    public int run(String[] args) throws Exception {
        // Создаю MapReduce задачу
        Job job = Job.getInstance(getConf(), "AvgRating");
        job.setJarByClass(AvgRating.class);
        // Устанавливаю класс отображения
        job.setMapperClass(VoteMapper.class);
        // Устанавливаю класс свертки
        job.setReducerClass(VoteAvgReducer.class);
        // На выходе как ключ будет id продукта
        job.setOutputKeyClass(Text.class);
        // На выходе как значение будет средняя оценка
        job.setOutputValueClass(DoubleWritable.class);

        // Путь до входного файла
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // Путь до выходной директории
        final Path output_dir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, output_dir);

        // Запускаю задачу и жду ее окончания
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        System.out.println("Start AvgRating");
        System.exit(ToolRunner.run(conf, new AvgRating(), args));
    }
}
