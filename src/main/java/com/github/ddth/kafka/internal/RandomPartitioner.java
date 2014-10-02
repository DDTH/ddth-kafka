package com.github.ddth.kafka.internal;

import java.util.Random;

import kafka.producer.Partitioner;

/**
 * A simple random {@link Partitioner} implementation.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 1.0.1
 */
public class RandomPartitioner implements Partitioner {

    private Random random = new Random(System.currentTimeMillis());

    /**
     * {@inheritDoc}
     */
    @Override
    public int partition(Object key, int numPartitions) {
        return random.nextInt(Integer.MAX_VALUE) % numPartitions;
    }

}
