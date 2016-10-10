package com.test.comparator;

import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

import static org.junit.Assert.*;

public class TupleComparatorByValTest {

    private TupleComparatorByVal tupleComparator = new TupleComparatorByVal();

    @Test
    public void compare() throws Exception {
        Tuple2<String, Double> t1 = new Tuple2<>("t1", 1.5);
        Tuple2<String, Double> t2 = new Tuple2<>("t2", 3.0);
        Assert.assertEquals(1, tupleComparator.compare(t1, t2));
    }

}