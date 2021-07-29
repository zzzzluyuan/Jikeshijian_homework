
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class flowbean implements Writable {
    private long upflow;
    private long downflow;
    private long sumflow;

    public long getUpflow() {
        return upflow;
    }

    public long getDownflow() {
        return downflow;
    }

    public long getSumflow() {
        return sumflow;
    }

    public void setUpflow(long upflow) {
        this.upflow = upflow;
    }

    public void setDownflow(long downflow) {
        this.downflow = downflow;
    }

    public void setSumflow(long sumflow) {
        this.sumflow = sumflow;
    }

    public flowbean(long upflow, long downflow) {
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow= upflow + downflow;
    }

    public flowbean(){}
    @Override
    public String toString() {
        return   "upflow=" + upflow +
                ", downflow=" + downflow +
                ", sumflow=" + sumflow ;
    }


    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.upflow);
        dataOutput.writeLong(this.downflow);
        dataOutput.writeLong(this.sumflow);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.upflow=dataInput.readLong();
        this.downflow=dataInput.readLong();
        this.sumflow=dataInput.readLong();

    }
}
