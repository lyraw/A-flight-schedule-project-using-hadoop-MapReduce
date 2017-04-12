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

public class Schedule {
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
                String uniqueCarrier = column[8];
                String arrDelay = column[14];
                // arrival on schedule (in 5 minutes)
                if (arrDelay.equals("NA") || Integer.valueOf(arrDelay) <= 5) {
                    word.set(uniqueCarrier);
                    result.set(0);
                    context.write(word, result);
                } else {
                    word.set(uniqueCarrier);
                    result.set(1);
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
            double delaySum = 0;
            double sum = 0;
            for (DoubleWritable val : values) {
                delaySum += val.get();
                sum += 1;
            }
            double rate = (sum - delaySum) / sum;
            result.set(rate);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Schedule <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "Schedule");
        job.setJarByClass(Schedule.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
