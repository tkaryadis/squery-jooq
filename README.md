# squery-jooq

SQuery to SQL with the power of [JOOQ](https://github.com/jOOQ/jOOQ)  
Write SQuery and generate SQL, or send the query directly for execution.

Clojure is functional general programming language with macros, allowing us to
create compact DSL's without the need to use extra languages in our code.

## Example

**SQuery**
```
(q :author
   (join :book (= :author.id :book.author_id))
   ((= :book.title "1984") (> :book.published_in 2008))
   (group :author.first_name :author.last_name)
   ((> (count-acc) 5))
   (sort :author.last_name)
   (limit 2)
   (skip 1)
   [:author.first_name :author.last_name (count-acc)])
```

**SQL**
```
SELECT AUTHOR.FIRST_NAME, AUTHOR.LAST_NAME, COUNT(*)
FROM AUTHOR
JOIN BOOK ON AUTHOR.ID = BOOK.AUTHOR_ID
WHERE BOOK.LANGUAGE = 'DE'
AND BOOK.PUBLISHED_IN > 2008
GROUP BY AUTHOR.FIRST_NAME, AUTHOR.LAST_NAME
HAVING COUNT(*) > 5
ORDER BY AUTHOR.LAST_NAME ASC NULLS FIRST
LIMIT 2
OFFSET 1
```

**JOOQ**
```
create.select(AUTHOR.FIRST_NAME, AUTHOR.LAST_NAME, count())
.from(AUTHOR)
.join(BOOK).on(BOOK.AUTHOR_ID.eq(AUTHOR.ID))
.where(BOOK.LANGUAGE.eq("DE"))
.and(BOOK.PUBLISHED_IN.gt(2008))
.groupBy(AUTHOR.FIRST_NAME, AUTHOR.LAST_NAME)
.having(count().gt(5))
.orderBy(AUTHOR.LAST_NAME.asc().nullsFirst())
.limit(2)
.offset(1);
```


## License

Copyright © 2022 FIXME

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
