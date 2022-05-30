package bdtc.lab1;

import lombok.extern.log4j.Log4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;

@Log4j
public class MapReduceApplication {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new RuntimeException("You should specify input and output folders!");
        }
        Configuration conf = new Configuration();
        // задаём выходной файл, разделенный запятыми - формат CSV в соответствии с заданием
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        FileSystem hdfs = FileSystem.get(conf);
        Job job = Job.getInstance(conf, "log levels count");
        job.setJarByClass(MapReduceApplication.class);
        job.setMapperClass(HW1Mapper.class);
        job.setReducerClass(HW1Reducer.class);
        job.setOutputKeyClass(CustomWritable.class); // CustomWritable as key
        job.setOutputValueClass(IntWritable.class); // int as value to sum up
        job.setOutputFormatClass(TextOutputFormat.class);

        Path outputDirectory = new Path(args[1]);
        if (hdfs.exists(outputDirectory)) {
            hdfs.delete(outputDirectory);
            log.info("Output directory exists, so deleting it");
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputDirectory);
        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
    }
}
