package software.ulpgc.bigData.InvertedIndex.BookApi;

public interface APISource {
    void start();
    APISource startServer();
    void stopServer();
}