package errorfilteringdemo.datum;

import org.apache.beam.sdk.transforms.DoFn;


/*
This class represents some basic, shared information between auditd log types.
(You can go down a huge rabbit hole trying to normalize auditd logs).

Note that Audit extends DoFn - this is a nice way to make Beam recognize how to serialize Audit and split it up between Beam workers.
 */
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
