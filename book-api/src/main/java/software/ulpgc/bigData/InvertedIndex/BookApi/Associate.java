package software.ulpgc.bigData.InvertedIndex.BookApi;

public class Associate {

    private Word word;
    private Book book;

    private int count;

    public Associate(Word word, Book book, int count) {
        this.word = word;
        this.book = book;
        this.count = count;
    }

    public Book getBook() {
        return book;
    }

    public int getCount() {
        return count;
    }
}