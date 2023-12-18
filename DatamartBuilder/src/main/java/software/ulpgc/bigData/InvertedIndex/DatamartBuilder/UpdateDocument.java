package software.ulpgc.bigData.InvertedIndex.DatamartBuilder;

import org.bson.conversions.Bson;

class UpdateDocument {
    private String wordLabel;
    private Bson update;

    public UpdateDocument(String wordLabel, Bson update) {
        this.wordLabel = wordLabel;
        this.update = update;
    }

    public String getWordLabel() {
        return wordLabel;
    }

    public Bson getUpdate() {
        return update;
    }
}
