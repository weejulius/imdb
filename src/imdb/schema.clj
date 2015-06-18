(ns imdb.schema
  {:doc "the shema for each kind of entity"})



;; the format of schema

(def schema-exmple
  {:name [:string :index :uniqure]
   :age [:int :index]})



(def schemas
  (atom {:user {:name [:string :index :uniqure]
                :event [:string :index]
                :age [:int :index]}
         :product {:title [:string :index]}}))


(defn piece-schema-def
  [entity-name piece-key]
  (get-in @schemas [entity-name piece-key]))


(defn piece-schema-def-validate
  [piece]
  )
