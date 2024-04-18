(defproject org.squery/squery-jooq "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.0"]
                 ;;[org.jooq/jooq "3.18.7"]  ;;old working
                 [org.jooq/jooq "3.19.7"]                   ;;new-semitested
                 [org.postgresql/postgresql "42.5.0"]
                 [com.mysql/mysql-connector-j "8.2.0"]
                 [org.flatland/ordered "1.5.9"]]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :repl-options {:init-ns squery-jooq.core})
