package software.ulpgc.bigData.InvertedIndex.DatamartBuilder;

public class Book {
    int id;
    String author;
    String title;
    String releaseYear;

    public Book(int id, String author, String title, String releaseYear) {
        this.id = id;
        this.author = author;
        this.title = title;
        this.releaseYear = releaseYear;
    }

    public int getId() {
        return id;
    }

    public String getAuthor() {
        return author;
    }

    public String getTitle() {
        return title;
    }

    public String getReleaseYear() {
        return releaseYear;
    }
}
