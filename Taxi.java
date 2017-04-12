import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Taxi {
    public static class MyMapper extends
            Mapper<Object, Text, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        private Text word = new Text();

        public void map(Object key, Text value,
                Mapper<Object, Text, Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            // exclude the first line
            if (!line.startsWith("Year")) {
                String column[] = line.split(",");
                String dest = column[17];
                String taxiIn = column[19];
                String taxiOut = column[20];

                int time = 0;
                if (!taxiIn.equals("NA")) {
                    time += Integer.valueOf(taxiIn);
                }
                if (!taxiOut.equals("NA")) {
                    time += Integer.valueOf(taxiOut);
                }
                if (time != 0) {
                    word.set(dest);
                    result.set(time);
                    context.write(word, result);
                }
            }
        }
    }

    public static class MyReducer extends
            Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(
                Text key,
                Iterable<DoubleWritable> values,
                Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            double count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            double avarageTime = sum / count;
            result.set(avarageTime);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Taxi <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "Taxi");
        job.setJarByClass(Taxi.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
