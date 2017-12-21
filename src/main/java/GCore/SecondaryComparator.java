package GCore;

import java.io.Serializable;
import java.util.Comparator;

/**
 * User:leen
 * Date:2017/12/15 0015
 * Time:9:38
 */
public class SecondaryComparator implements Comparator<String>, Serializable {
    @Override
    public int compare(String o1, String o2) {
        String[] oo1 = o1.split(" ");
        String[] oo2 = o2.split(" ");
        if (oo1[0].equals(oo2[0])) {
            return -Integer.valueOf(oo1[1]).compareTo(Integer.valueOf(oo2[1]));
        } else {
            return oo1[0].compareTo(oo2[0]);
        }
    }
}
