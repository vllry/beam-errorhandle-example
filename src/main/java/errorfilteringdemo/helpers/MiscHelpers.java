package errorfilteringdemo.helpers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class MiscHelpers {
    public static String regexHelper(String regex, String text) {
        Matcher matcher = Pattern.compile(regex).matcher(text);
        matcher.find();
        return matcher.group(1);
    }
}
