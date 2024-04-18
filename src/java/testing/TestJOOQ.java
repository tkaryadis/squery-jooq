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