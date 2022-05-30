package bdtc.lab1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import org.apache.hadoop.io.WritableComparable;
public class CustomWritable implements WritableComparable<CustomWritable> {
    private final static SimpleDateFormat formatter = new SimpleDateFormat( "MMM dd / HH" );
    private Date logDate;
    private CounterType logLevel;
    public CustomWritable()
    {
        this.logDate = new Date(0);
        this.logLevel = CounterType.INVALID;
    }
    public Date getDate()
    {
        return logDate;
    }
    public void setDate( Date d )
    {
        this.logDate = d;
    }
    public CounterType getLogLevel() {
        return logLevel;
    }
    public void setLogLevel(CounterType logLevel) {
        this.logLevel = logLevel;
    }
    public void setLogLevel(int logLevel) {
        this.logLevel = CounterType.values()[logLevel];
    }
    @Override
    public void readFields(DataInput rhs) throws IOException {
        logDate = new Date(rhs.readLong());
        logLevel = CounterType.values()[rhs.readInt()];
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(logDate.getTime());
        int index = Arrays.binarySearch(CounterType.values(), logLevel);
        if (index < 0 || index > 7)
            out.writeInt(8); // invalid
        else
            out.writeInt(index);
    }
    public String toString(){
        return formatter.format(logDate) + ":00 | Level: " + logLevel.name();
    }
    @Override
    public int hashCode() {
        return logDate.hashCode() + logLevel.hashCode();
    }
    @Override
    public boolean equals(Object o) {
        if (o instanceof CustomWritable) {
            CustomWritable cw = (CustomWritable) o;
            return logDate.equals(cw.logDate) && logLevel.equals(cw.logLevel);
        }
        return false;
    }

    public int compareTo(CustomWritable rhs) {
        int cmp = logDate.compareTo(rhs.logDate);
        if (cmp != 0) {
            return cmp;
        }
        return logLevel.compareTo(rhs.logLevel);
    }
}