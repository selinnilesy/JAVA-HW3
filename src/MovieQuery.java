import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MovieQuery {
       public static void queryAll(String inFileName, String outFileName) {
        List<String> rawData;
        try{
            Path path = Paths.get(System.getProperty("user.dir") + "/" + inFileName + ".csv");
            rawData = Files.lines(path).collect(Collectors.toList());
        } catch (Exception e) {
            System.out.println("Wrong file path : " + inFileName);
            return;
        }
        System.out.println(rawData);
        // get rid of headers as it is still a collection : Title,Year,Genre,RunTime,Rating,Votes,Director,Cast,Gross
        // also transform each line into array for queries to use stream<String[]>.
        rawData.remove(0);
        List<String[]> preparedData = rawData.stream().map(line -> line.split(",")).collect(Collectors.toList());
        query1(preparedData,System.getProperty("user.dir") + "/"+ outFileName + "-1.out");
        query2(preparedData, System.getProperty("user.dir") + "/"+ outFileName + "-2.out");
        query3(preparedData, System.getProperty("user.dir") + "/"+ outFileName + "-3.out");
        query4(preparedData, System.getProperty("user.dir") + "/"+ outFileName + "-4.out");
        query5(preparedData, System.getProperty("user.dir") + "/"+ outFileName + "-5.out");
    }

    public static void query1(List<String[]> collection, String outFileName){
        PrintWriter outputFile_1 =  QueryOutputWriter.processOutput(outFileName, MovieQuery::outputQuery);
        formatStreamForOutput(
                StreamProcessor.applyLambdaToStream(collection.stream(), MovieQuery::fetchTitles)
            ).forEach(outputFile_1::println);
        outputFile_1.close();
    }
    public static void query2(List<String[]> collection, String outFileName){
        PrintWriter outputFile_2 =  QueryOutputWriter.processOutput(outFileName, MovieQuery::outputQuery);
        formatStreamForOutput(
                StreamProcessor.applyLambdaToStream(collection.stream(), MovieQuery::fetchDirectorsWithHighRatings)
            ).forEach(outputFile_2::println);
        outputFile_2.close();
    }
    public static void query3(List<String[]> collection, String outFileName){
        PrintWriter outputFile_3 =  QueryOutputWriter.processOutput(outFileName, MovieQuery::outputQuery);
        formatStreamForOutput(
                StreamProcessor.applyLambdaToStream(collection.stream(), MovieQuery::fetchAdventureWithLeastRevenue)
            ).forEach(outputFile_3::println);
        outputFile_3.close();
    }
    public static void query4(List<String[]> collection, String outFileName){
        PrintWriter outputFile_4 =  QueryOutputWriter.processOutput(outFileName, MovieQuery::outputQuery);

        List<String> succesfull_ones = StreamProcessor.applyLambdaToStream(collection.stream(), MovieQuery::fetchDirectorsWithHighRatings).collect(Collectors.toList());
        List<String> failed_ones =StreamProcessor.applyLambdaToStream(collection.stream(), MovieQuery::fetchDirectorsWithLowRatings).collect(Collectors.toList());
        // local variables can be used but not modified
        formatStreamForOutput(succesfull_ones.stream()
                                            .filter(failed_ones::contains)
                            ).forEach(outputFile_4::println);
        outputFile_4.close();
    }
    public static void query5(List<String[]> collection, String outFileName){
        PrintWriter outputFile_5 =  QueryOutputWriter.processOutput(outFileName, MovieQuery::outputQuery);
        List<String> director_earliest = collection.stream()
                                                    .min(Comparator.comparingInt(arr -> Integer.parseInt(arr[1])))
                                                    .stream()
                                                    .map(arr -> arr[6]).collect(Collectors.toList());
        Integer sum = collection.stream()
                .filter(arr -> arr[6].equals(director_earliest.get(0)))
                .map(arr -> Integer.parseInt(arr[3]))
                .reduce(0, Integer::sum);
        outputFile_5.println(sum);

        outputFile_5.close();
    }

    public static Stream<String> fetchAdventureWithLeastRevenue(Stream<String[]> stream) {
        return stream.filter(arr -> (arr[2].contains("Adventure") &&
                        !Objects.equals(arr[8], "-1")))
                .distinct()
                .sorted(Comparator.comparingDouble(arr -> Double.parseDouble(arr[8])))
                .findFirst().stream()
                .map(arr -> arr[6]);
    }
    public static Stream<String> fetchTitles(Stream<String[]> stream) {
        return stream.map( arr -> arr[0]);
    }
    public static Stream<String> fetchDirectorsWithHighRatings(Stream<String[]> stream) {
        return stream.filter(x-> Double.parseDouble(x[4]) >= 8.5
                )
                .map( arr -> arr[6])
                .distinct();
    }
    public static Stream<String> fetchDirectorsWithLowRatings(Stream<String[]> stream) {
        return stream.filter(x-> Double.parseDouble(x[4]) <= 8.0)
                .map( arr -> arr[6])
                .distinct();
    }
    public static PrintWriter outputQuery(String outFileName){
        try{
            return new PrintWriter(Files.newBufferedWriter(Paths.get(outFileName)), true);
        }
        catch(Exception e){
            System.out.println(e);
            return null;
        }
    }

    public static Stream<String> formatStreamForOutput(Stream<String> stream){
        return stream.map(String::toUpperCase)
                .sorted();
    }
}

