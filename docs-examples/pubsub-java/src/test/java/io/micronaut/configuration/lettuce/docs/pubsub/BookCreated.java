package io.micronaut.configuration.lettuce.docs.pubsub;

import io.micronaut.core.annotation.Introspected;

@Introspected
public class BookCreated {
    private String title;
    private String author;

    public BookCreated() {
    }

    public BookCreated(String title, String author) {
        this.title = title;
        this.author = author;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }
}
