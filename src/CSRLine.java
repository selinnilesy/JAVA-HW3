import java.util.stream.Stream;

@FunctionalInterface
public interface CSRLine {
    double applyFetch(String[] line);

    public static double applyFetchToStreamLine(String[] line, CSRLine lambdaFunc) {
        return lambdaFunc.applyFetch(line);
    }
}
