package errorfilteringdemo.datum;


import org.apache.beam.sdk.transforms.DoFn;

import java.util.Arrays;

public class Failure extends DoFn {

    private String failedClass;
    private String message;
    private String precursorDataString;
    private String stackTrace;  // May want to expand to an Array/ArrayList depending on use case.

    public Failure(Object precursorData, Object datum, Throwable thrown) {
        this.failedClass = datum.getClass().toString();
        this.message = thrown.toString();
        this.precursorDataString = precursorData.toString();
        this.stackTrace = Arrays.toString(thrown.getStackTrace());
    }

    @Override
    public String toString() {
        return "{" +
                "\nfailedClass: " + this.failedClass +
                "\nmessage: " + this.message +
                "\nprecursorDataString: " + this.precursorDataString +
                "\nstackTrace: " + this.stackTrace +
                "\n}";
    }

}
