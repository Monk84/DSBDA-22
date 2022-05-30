package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

public class HW1Mapper extends Mapper<LongWritable, Text, CustomWritable, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final CustomWritable cw = new CustomWritable(); // inner key
    private static final SimpleDateFormat formatter = new SimpleDateFormat("MMM dd HH");
    private static final String delimiter = ","; // delimiter in the syslog line

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tk = new StringTokenizer(line, delimiter); // tokenizing given string
        if (tk.countTokens() >= 3) { // sanity check
            int logLevel = -1;
            try {
                logLevel = Integer.parseInt(tk.nextToken());
            } catch (NumberFormatException ex) {
                cw.setLogLevel(CounterType.INVALID);
            }
            if (logLevel < 0 || logLevel > 7) { // sanity check
                cw.setLogLevel(CounterType.INVALID);
            } else {
                cw.setLogLevel(logLevel);
            }
            try {
                cw.setDate(formatter.parse(tk.nextToken()));
            } catch (ParseException ex) {
                cw.setDate(new Date(0)); // some null date
            }
        }
        if (cw.getLogLevel() != CounterType.INVALID) { // silently ignoring invalid lines
            context.write(cw, one);
        }
    }
}


