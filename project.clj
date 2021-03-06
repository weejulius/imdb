(defproject imdb "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [pandect "0.5.2"]
                 [common-clj "1.0.0-SNAPSHOT"]
                 [org.clojure/tools.namespace "0.2.10"]
                 [factual/clj-leveldb "0.1.1"]
                 [org.mapdb/mapdb "2.0-beta1"]
                 [org.clojure/test.check "0.7.0"]
                 [it.unimi.dsi/fastutil "7.0.6"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]]
  :repl-options {:init (load-file "src/imdb/warmup.clj")})
