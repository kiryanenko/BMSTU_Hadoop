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

// Запуск hadoop:
// /usr/local/Cellar/hadoop/3.2.1/sbin/start-dfs.sh
// /usr/local/Cellar/hadoop/3.2.1/sbin/start-yarn.sh
// mapred --daemon start historyserver

// Команды для старта:
// hadoop dfs -rm -r -f  /prodname_avg_rating
// yarn jar ./out/artifacts/HW1/HW1.jar ProdNameAvgRating -D mapred.textoutputformat.separator="," -D mapreduce.job.reduces=2 /avg_rating/ /meta_Electronics.json /prodname_avg_rating

public class ProdNameAvgRating  extends Configured implements Tool {

    // Writable класс для объеденения записей названия и рейтинга
    public static class JoinProdNameAvgRating implements Writable {
        static final byte PROD_NAME_TYPE = 'n';
        static final byte AVG_RATING_TYPE = 'r';

        // Тип записи [Название | Рейтинг]
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
            // Записываю тип
            dataOutput.writeByte(type);
            // В зависимомти от типа записываю название или рейтинг
            if (type == PROD_NAME_TYPE) {
                dataOutput.writeChars(prodName);
            } else {
                dataOutput.writeDouble(avgRating);
            }
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            // Считываю тип
            type = dataInput.readByte();
            // В зависимомти от типа считываю название или рейтинг
            if (type == PROD_NAME_TYPE) {
                prodName = dataInput.readLine();
            } else {
                avgRating = dataInput.readDouble();
            }
        }
    }


    // Класс отображения для объеденения записей рейтинга и названия продукта
    // На вход подаются строки из файла с записями рейтингов продуктов
    // Формат записей: id,rating
    // На выход отдает ключ -id продукта и значение - рейтинг с указанием типа
    public static class AvgRatingMapper extends Mapper<LongWritable, Text, Text, JoinProdNameAvgRating> {

        private final Text id = new Text();
        private final JoinProdNameAvgRating join = new JoinProdNameAvgRating();

        // Функция отображения, которая вызывается для каждой записи с рейтингом продукта
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Разбиваю запись по запятой
            // id = data[0]
            // rating = data[1]
            String[] data = value.toString().split(",", 2);

            // Если запись невалидная, то прерываю
            if (data.length != 2) {
                return;
            }

            id.set(data[0]);

            double rating;
            try {
                // Распарсиваю рейтинг
                rating = Double.parseDouble(data[1]);
            } catch (NumberFormatException ignored) {
                return;
            }
            // Устанавлиаю рейтинг
            join.setAvgRating(rating);

            // На выход отдаю id и рейтинг
            context.write(id, join);
        }
    }

    // Класс отображения для объеденения записей рейтинга и названия продукта
    // На вход подаются строки из файла с записями названий продуктов в формате JSON
    // На выход отдает ключ - id продукта и значение - название с указанием типа
    public static class ProdNameMapper extends Mapper<LongWritable, Text, Text, JoinProdNameAvgRating> {

        private final JSONParser jsonParser = new JSONParser();
        private final Text id = new Text();
        private final JoinProdNameAvgRating join = new JoinProdNameAvgRating();

        // Функция отображения, которая вызывается для каждой записи с названием продукта
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String buf = value.toString();
                // Из-за того что в JSON строки обернуты одинарными кавычками, не удается распарсить json
                // Для решения этой проблемы заменяю одинарные ковычки - двойными
                buf = buf.replace('\'', '"');
                // Распарсиваю JSON
                JSONObject json = (JSONObject) jsonParser.parse(buf);
                // Достаю id продукта
                String asin = (String) json.get("asin");
                // Достаю название продукта
                String title = (String) json.get("title");
                // Если были получены id и название продукта, то отдаю их на выход
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


    // Класс свертки для объеденения записей рейтинга и названия продукта
    // На вход как ключ подается id продукта и 2 значения - название, рейтинг
    // На выход отдает ключ - id продукта и значение - название,рейтинг
    public static class ProdNameAvgReducer extends Reducer<Text, JoinProdNameAvgRating, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<JoinProdNameAvgRating> values, Context context)
                throws IOException, InterruptedException {
            String name = null;
            Double rating = null;

            for (JoinProdNameAvgRating join : values) {
                // Определяю значение является названием или рейтингом и достаю их
                if (join.getType() == JoinProdNameAvgRating.PROD_NAME_TYPE) {
                    name = join.getProdName();
                } else {
                    rating = join.getAvgRating();
                }
            }

            // Если все в порядке, то отдаю id,название,рейтинг
            if (name != null && rating != null) {
                result.set(name + ',' + rating);
                context.write(key, result);
            }
        }
    }

    public int run(String[] args) throws Exception {
        // Создаю MapReduce задачу
        Job job = Job.getInstance(getConf(), "ProdNameAvgRating");
        job.setJarByClass(ProdNameAvgRating.class);
        // На выходе отображения как ключ будет id продукта
        job.setMapOutputKeyClass(Text.class);
        // На выходе отображения как значение будет либо название или рейтинг
        job.setMapOutputValueClass(JoinProdNameAvgRating.class);
        // Устанавливаю класс свертки
        job.setReducerClass(ProdNameAvgReducer.class);
        // На выходе как ключ будет id продукта
        job.setOutputKeyClass(Text.class);
        // На выходе как значение будет название,рейтинг
        job.setOutputValueClass(Text.class);

        final Path avgRatingPath = new Path(args[0]);
        final Path prodNamePath = new Path(args[1]);
        final Path outputPath = new Path(args[2]);

        // Для файлов устанавливаю соответствующие классы отображения
        MultipleInputs.addInputPath(job, avgRatingPath, TextInputFormat.class, AvgRatingMapper.class);
        MultipleInputs.addInputPath(job, prodNamePath, TextInputFormat.class, ProdNameMapper.class);

        // Путь до выходной директории
        FileOutputFormat.setOutputPath(job, outputPath);

        // Запускаю задачу и жду ее окончания
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        System.out.println("Start ProdNameAvgRating");
        System.exit(ToolRunner.run(conf, new ProdNameAvgRating(), args));
    }
}
