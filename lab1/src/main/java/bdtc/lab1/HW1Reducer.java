package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Редьюсер: суммирует все единицы полученные от {@link HW1Mapper}
 */

public class HW1Reducer extends Reducer<CustomWritable, IntWritable, CustomWritable, IntWritable> {

    @Override
    protected void reduce(CustomWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get(); // reducing by sum
        }
        context.write(key, new IntWritable(sum));
    }
}
