import java.util.stream.Stream;

public interface StreamProcessor {
    Stream<String> processStream(Stream<String> strings);

    public static Stream<String> applyLambdaToStream(Stream<String> stream,StreamProcessor lambdaFunc) {
        return lambdaFunc.processStream(stream);
    }
}