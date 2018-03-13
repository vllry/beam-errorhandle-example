package errorfilteringdemo.datum;


import org.apache.beam.sdk.transforms.DoFn;

import java.util.Arrays;

public class Failure extends DoFn {

    private String message;
    private String stackTrace;  // May want to expand to an Array/ArrayList depending on use case.

    public Failure(Object datum, Throwable thrown) {
        this.message = thrown.toString();
        this.stackTrace = Arrays.toString(thrown.getStackTrace());

        System.out.println("FAILURE CAUGHT: " + this.message);
    }

    @Override
    public String toString() {
        return "{" +
                "\nmessage: " + this.message +
                "\nstackTrace: " + this.stackTrace +
                "\n}";
    }

}
