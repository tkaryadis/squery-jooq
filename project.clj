(defproject squery-jooq "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]

                 [org.jooq/jooq "3.17.4"]

                 [org.postgresql/postgresql "42.5.0"]

                 [org.flatland/ordered "1.5.9"]
                 ]
  :repl-options {:init-ns squery-jooq.core})
