(ns shred.stitch)


(defn stitch [ms ns path km many?]
  (let [get-ks (fn [m] (map #(get-in m %) (keys km)))
        get-vs (fn [m] (map #(get    m %) (vals km)))
        lookup (group-by get-vs ns)]
    (for [m ms]
      (let [values (get lookup (get-ks m))
            values (if-not many? (first values) values)]
        (assoc-in m path values)))))
