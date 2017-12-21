package GCore;

import java.io.Serializable;
import java.util.Comparator;

/**
 * User:leen
 * Date:2017/12/14 0014
 * Time:15:17
 */

/*
public class MyComparator implements Serializable {
    public static Comparator comparator(){
        Comparator<String> comparator = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return Integer.valueOf(o1).compareTo(Integer.valueOf(o2));
            }
        };
        return comparator;
    }
}
*/

public class MyComparator implements Comparator<String>,Serializable {

    @Override
    public int compare(String o1, String o2) {
        return Integer.valueOf(o1).compareTo(Integer.valueOf(o2));
    }
}

