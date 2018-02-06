package errorfilteringdemo.datum;


import java.util.Arrays;

public class Failure {

    private String message;
    private String stackTrace;  // May want to flatten to a multiline string depending on use case.

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
