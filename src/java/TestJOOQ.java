/*
import static org.jooq.SQLDialect.POSTGRES;
import static org.jooq.impl.DSL.*;
import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.*;
import org.jooq.util.postgres.PostgresDSL;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

//CREATE TABLE book (
//  id              INTEGER       PRIMARY KEY,
//  author_id       INTEGER      NOT NULL,
//  title           VARCHAR(400) NOT NULL,
//  published_in    INTEGER      NOT NULL,
//  language_id     INTEGER      NOT NULL,
//
//  CONSTRAINT fk_book_author   FOREIGN KEY (author_id)   REFERENCES author(id),
//  CONSTRAINT fk_book_language FOREIGN KEY (language_id) REFERENCES language(id)
//);

class Book {
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

public class TestJOOQ
{
    public static void main(String[] args)
    {
        try {
            String cstring = new String(Files.readAllBytes(
                    Paths.get("/home/white/IdeaProjects/squery/squery-jooq/authentication/connection-string")));
            Connection c = DriverManager.getConnection(cstring);
            DSLContext create = DSL.using(c, SQLDialect.POSTGRES);

            Result<Record> r=create.select(DSL.asterisk(),DSL.val(100).as("newField"))
                    .from(DSL.table("author"))
                    .fetch();
            System.out.println(r);

            //;;BOOK.TITLE.eq(any("Animal Farm", "1982"));
            //;BOOK.PUBLISHED_IN.gt(all(1920, 1940));

            Result<Record> r1=create.select()
                    .from(DSL.table("book"))
                    //.where(DSL.field("title").eq(any("Animal Farm", "1982")))
                    .fetch();
            System.out.println(r1);

            //POJO
            List<Book> r2=create.select()
                    .from(DSL.table("book"))
                    .fetchInto(Book.class);
            r2.stream().forEach(System.out::println);


        }
        catch(Exception e)
        {
            System.out.println(e.getMessage());

        }
    }
}

 */