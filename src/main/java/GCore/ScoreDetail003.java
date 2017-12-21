package GCore;

import java.io.Serializable;

/**
 * User:leen
 * Date:2017/12/11 0011
 * Time:16:59
 */

public class ScoreDetail003 implements Serializable {
    String name ;
    String subject ;
    int score ;

    public ScoreDetail003(String name, String subject, int score) {
        this.name = name;
        this.subject = subject;
        this.score = score;
    }

}
