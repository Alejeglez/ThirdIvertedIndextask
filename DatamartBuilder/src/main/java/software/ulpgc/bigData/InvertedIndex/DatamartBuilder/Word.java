package software.ulpgc.bigData.InvertedIndex.DatamartBuilder;

public class Word {
    int id;
    String label;

    public Word(int id, String label) {
        this.id = id;
        this.label = label;
    }

    public int getId() {
        return id;
    }

    public String getLabel() {
        return label;
    }

}
