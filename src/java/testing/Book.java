package testing;

public class Book {
    private Integer id;
    private Integer author_id;
    private String title;
    private Integer published_in;
    private Integer language_id;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getAuthor_id() {
        return author_id;
    }

    public void setAuthor_id(Integer author_id) {
        this.author_id = author_id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Integer getPublished_in() {
        return published_in;
    }

    public void setPublished_in(Integer published_in) {
        this.published_in = published_in;
    }

    public Integer getLanguage_id() {
        return language_id;
    }

    public void setLanguage_id(Integer language_id) {
        this.language_id = language_id;
    }

    @Override
    public String toString() {
        return "Book{" +
                "id=" + id +
                ", author_id=" + author_id +
                ", title='" + title + '\'' +
                ", published_in=" + published_in +
                ", language_id=" + language_id +
                '}';
    }
}
