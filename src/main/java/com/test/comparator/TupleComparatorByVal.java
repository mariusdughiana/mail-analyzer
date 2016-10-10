package com.test.comparator;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class TupleComparatorByVal implements Comparator<Tuple2<String, Double>>, Serializable {
    // assuming that the second field of Tuple2 is a count or frequency
    public int compare(Tuple2<String, Double> t1,
                       Tuple2<String, Double> t2) {
        return -t1._2.compareTo(t2._2);    // sort descending
        // return t1._2.compareTo(t2._2);  // sort ascending
    }
}