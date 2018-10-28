/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Kafka;

import static Kafka.SparkKafkaStreaming.MAX_PLAYER;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Dream
 */
public class VoteDriver implements Runnable {

    public static final int VOTE_MIN_WINDOW = 120;

    public VoteDriver(int user) {

        this.user = user;

    }

    public Long getUnixTime() {
        Date now = new Date();
        Long longTime = now.getTime() / 1000;
        return longTime;
    }
    private Thread t;
    int counter = 0;
    Long firstVoteTime = null;

    int user;

    public static void main(String arg[]) throws InterruptedException {
        VoteDriver v = new VoteDriver(1);
        VoteDriver v1 = new VoteDriver(2);
        VoteDriver v2 = new VoteDriver(3);
        VoteDriver v3 = new VoteDriver(4);
        VoteDriver v4 = new VoteDriver(5);
        VoteDriver v5 = new VoteDriver(6);
        VoteDriver v6 = new VoteDriver(7);
        v.start();
        v1.start();
        v2.start();
        v3.start();
        v4.start();
        v5.start();
        v6.start();
        /*while (true) {
         int randVote = ThreadLocalRandom.current().nextInt(1, MAX_PLAYER);
         Thread.sleep(10000);
         System.out.println(v.castVote(randVote));
         }*/
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

    public void start() {

        if (t == null) {
            t = new Thread(this);
            t.start();
        }
    }

    @Override
    public void run() {

        while (true) {
            int randVote = ThreadLocalRandom.current().nextInt(1, MAX_PLAYER);
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ex) {
                Logger.getLogger(VoteDriver.class.getName()).log(Level.SEVERE, null, ex);
            }
            SparkKafkaStreaming.runProducer(this.castVote(randVote));
        }
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
