import bdtc.lab1.CounterType;
import bdtc.lab1.CustomWritable;
import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;


public class MapReduceTest {

    private MapDriver<LongWritable, Text, CustomWritable, IntWritable> mapDriver;
    private ReduceDriver<CustomWritable, IntWritable, CustomWritable, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, CustomWritable, IntWritable, CustomWritable, IntWritable> mapReduceDriver;

    private final String testLogLine0 = "0,APR 23 12:45:31,emergency";
    private final String testLogLine1 = "1,JAN 12 01:02:03,alert";
    private final String testLogLine2 = "2,MAR 3 10:31:45,critical";
    private final String testLogLine3 = "3,JUL 30 3:35:21,error";
    private final String testLogLine4 = "4,DEC 21 23:12:01,warning";
    private final String testLogLine5 = "5,SEP 1 12:34:56, notice";
    private final String testLogLine6 = "6,MAY 21 6:45:00, info";
    private final String testLogLine7 = "7,APR 26 3:33:33, dbg";
    private CustomWritable cw;
    private SimpleDateFormat formatter;
    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        cw = new CustomWritable(); // used in all tests
        formatter = new SimpleDateFormat( "MMM dd HH" ); // used in all tests
    }

    @Test
    // Mapper must output log with 0 level and same date/hour as in sample
    public void testMapper() throws IOException {
        cw.setDate(formatter.parse(testLogLine0, new ParsePosition(2)));
        cw.setLogLevel(CounterType.LOG_LEVEL_0);
        mapDriver
                .withInput(new LongWritable(), new Text(testLogLine0))
                .withOutput(cw, new IntWritable(1))
                .runTest();
    }

    @Test
    // Reduced must output sum of two 1s which have same key
    public void testReducer() throws IOException {
        cw.setDate(formatter.parse(testLogLine1, new ParsePosition(2)));
        cw.setLogLevel(CounterType.LOG_LEVEL_1);
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver
                .withInput(cw, values)
                .withOutput(cw, new IntWritable(2))
                .runTest();
    }

    @Test
    // MapReduce must output 2 and level 2
    public void testMapReduce() throws IOException {
        cw.setDate(formatter.parse(testLogLine2, new ParsePosition(2)));
        cw.setLogLevel(CounterType.LOG_LEVEL_2);
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testLogLine2))
                .withInput(new LongWritable(), new Text(testLogLine2))
                .withOutput(cw, new IntWritable(2))
                .runTest();
    }
}
