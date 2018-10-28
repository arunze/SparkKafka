/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Kafka;

import static Kafka.SparkKafkaStreaming.MAX_PLAYER;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * @author Dream
 */
public class Vote {

    public static final int VOTE_MIN_WINDOW = 120;

    public Vote(int user) {
        this.user = user;
    }

    public Long getUnixTime() {
        Date now = new Date();
        Long longTime = now.getTime() / 1000;
        return longTime;
    }

    int counter = 0;
    Long firstVoteTime = null;

    int user;

    public static void main(String arg[]) throws InterruptedException {
        Vote v = new Vote(1);
        while (true) {
            int randVote = ThreadLocalRandom.current().nextInt(1, MAX_PLAYER);
            Thread.sleep(10000);
            //System.out.println(v.castVote(randVote));
            SparkKafkaStreaming.runProducer(v.castVote(randVote));
        }
    }

    public String castVote(int vote) {
        long currentTime = this.getUnixTime();
        int count = this.getCounter(currentTime);
        return user + "," + vote + "," + count + "," + currentTime;

    }

    public int getCounter(Long currentTimeStamp) {

        if (firstVoteTime == null) {
            firstVoteTime = currentTimeStamp;
            counter = 0;
            return 0;

        } else if (currentTimeStamp - firstVoteTime < VOTE_MIN_WINDOW) {
            counter = counter + 1;
            return counter;
        } else {
            firstVoteTime = currentTimeStamp;
            counter = 0;
            return counter;
        }

    }

}
