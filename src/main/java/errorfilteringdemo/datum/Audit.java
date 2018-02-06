package errorfilteringdemo.datum;

import org.apache.beam.sdk.transforms.DoFn;


public class Audit extends DoFn {

    public String type;
    public Integer pid;
    public Integer uid;
    public Integer auid;

    @Override
    public String toString() {
        return "{" +
                "\ntype: " + this.type +
                "\npid: " + this.pid +
                "\nuid: " + this.uid +
                "\nauid: " + this.auid +
                "\n}";
    }
}
