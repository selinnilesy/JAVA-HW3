import java.util.List;
import java.util.stream.Stream;

@FunctionalInterface
public interface StreamProcessor {
    Stream<String> processStream(Stream<String[]> strings);

    public static Stream<String> applyLambdaToStream(Stream<String[]> stream,StreamProcessor lambdaFunc) {
        return lambdaFunc.processStream(stream);
    }
}

