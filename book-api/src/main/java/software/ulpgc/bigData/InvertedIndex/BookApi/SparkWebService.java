package software.ulpgc.bigData.InvertedIndex.BookApi;

import com.google.gson.Gson;
import spark.Request;
import spark.Response;

import java.text.SimpleDateFormat;
import java.util.*;

import static spark.Spark.*;

public class SparkWebService implements APISource{
    private final DataManagerMongo dataManager;
    private final Gson gson = new Gson();
    public APISource startServer() {
        port(8080);
        return this;
    }
    public SparkWebService() {
        this.dataManager = new DataManagerMongo();
    }
    public void start() {
        get("/documents/books", this::getBooksRequest);
        get("/documents/words", this::getWordsRequest);
        get("/documents/associations", this::getAssociationsRequest);
        get("/documents/:words", (request, response) -> {


            String wordsParam = request.params(":words");
            String author = request.queryParams("author");
            String fromYear = request.queryParams("from");
            String toYear = request.queryParams("to");
            if (wordsParam == null || wordsParam.trim().isEmpty()) {
                response.status(400);
                return "Parameter 'words' is required";
            }
            String[] wordsArray = wordsParam.split("\\+");
            List<HashMap> listResults  = new ArrayList<>();
            for (String keyWord : wordsArray){
                HashMap<String, List<Associate>> results = dataManager.searchBooks(keyWord, author, fromYear, toYear);
                listResults.add(results);
            }

            Date now = new Date();
            SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            String timestampString = dateFormat.format(now);

            System.out.println(timestampString + "...words:" + Arrays.toString(wordsArray));

            response.type("application/json");
            return gson.toJson(listResults);
        });

        exception(Exception.class, (exception, request, response) -> {
            exception.printStackTrace();
            response.status(500);
            response.body("Internal Server Error");
        });

    }
    @Override
    public void stopServer() {
        stop();
    }
    private Object getBooksRequest(Request request, Response response) {
        List<Book> books = dataManager.getAllBooks();
        response.type("application/json");
        return gson.toJson(books);
    }
    private Object getWordsRequest(Request request, Response response) {
        List<Word> words = dataManager.getAllWords();
        response.type("application/json");
        return gson.toJson(words);
    }

    private Object getAssociationsRequest(Request request, Response response) {
        List<Associate> associations = dataManager.getAllAssociations();
        response.type("application/json");
        return gson.toJson(associations);
    }
}