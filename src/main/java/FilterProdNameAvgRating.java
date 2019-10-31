// Напишите программу, которая выводит средний рейтинги всех продуктов из "prodname_avg_rating.csv",
// в названии которых встречается введенное при запуске слово: ProdId,Name,Rating

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

// Запуск hadoop:
// /usr/local/Cellar/hadoop/3.2.1/sbin/start-dfs.sh
// /usr/local/Cellar/hadoop/3.2.1/sbin/start-yarn.sh
// mapred --daemon start historyserver

// Команды для старта:
// hadoop dfs -rm -r -f /filter_prod_name_avg_rating
// yarn jar ./out/artifacts/HW1/HW1.jar FilterProdNameAvgRating -D mapred.textoutputformat.separator="," -D mapreduce.job.reduces=2 canon /prodname_avg_rating /filter_prod_name_avg_rating

public class FilterProdNameAvgRating  extends Configured implements Tool {

    // Класс отображения, на вход подаются строки из файла с записью вида: id,name,rating
    // На выход отдает записи продуктов, которые содержат искомую строку
    public static class FilterProdNameAvgRatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String search;
        private final Text id = new Text();
        private final Text nameRating = new Text();

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            // Из конфига достаю искомую строку
            search = conf.get("search").toLowerCase();
        }

        // Функция отображения, которая вызывается для каждой записи
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Разбиваю запись по запятой
            // id = data[0]
            // name = data[1]
            // rating = data[2]
            String[] data = value.toString().split(",", 3);
            // Если запись содержит искомую строку, то отдаю ее на выход
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
        // Устанавливаю в конфиг искомую строку
        conf.set("search", search);

        // Создаю MapReduce задачу
        Job job = Job.getInstance(conf, "FilterProdNameAvgRating");
        job.setJarByClass(FilterProdNameAvgRating.class);
        // Устанавливаю класс отображения
        job.setMapperClass(FilterProdNameAvgRatingMapper.class);
        // На выходе как ключ будет id продукта
        job.setOutputKeyClass(Text.class);
        // На выходе как значение будет название, средняя оценка
        job.setOutputValueClass(Text.class);

        // Путь до входной директории
        FileInputFormat.addInputPath(job, inputPath);
        // Путь до выходной директории
        FileOutputFormat.setOutputPath(job, outputPath);

        // Запускаю задачу и жду ее окончания
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        System.out.println("Start FilterProdNameAvgRating");
        System.exit(ToolRunner.run(conf, new FilterProdNameAvgRating(), args));
    }
}
