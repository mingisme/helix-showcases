package com.swang.helix;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.helix.lock.DistributedLock;
import org.apache.helix.lock.LockInfo;
import org.apache.helix.lock.LockScope;
import org.apache.helix.lock.helix.HelixLockScope;
import org.apache.helix.lock.helix.ZKDistributedNonblockingLock;

import java.util.Arrays;

public class PriorityWorker extends Thread{

    private final String zkAddress;
    private final int priority;

    public PriorityWorker(String name, String zkAddress, int priority) {
        super(name);
        this.zkAddress = zkAddress;
        this.priority = priority;
    }

    @Override
    public void run() {

        LockScope lockScope = new HelixLockScope(HelixLockScope.LockScopeProperty.RESOURCE, Arrays.asList("test1","lock1"));

        DistributedLock lock = new ZKDistributedNonblockingLock.Builder()
                .setPriority(this.priority)
                .setCleanupTimeout(100L)
                .setIsForceful(true)
                .setLockListener(()->{
                    System.out.println("clean up " + getName() );
                })
                .setLockMsg("Lock by " + getName())
                .setLockScope(lockScope)
                .setTimeout(10000L)
                .setUserId(getName())
                .setZkAddress(this.zkAddress).build();

        try {
            if (lock.tryLock()) {

                System.out.println(this.getName() + " execute task");
                LockInfo currentLockInfo = lock.getCurrentLockInfo();
                ObjectMapper objectMapper = new ObjectMapper();
                System.out.println(objectMapper.writeValueAsString(currentLockInfo));

                try {
                    Thread.sleep(10000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } finally {
            if(lock.isCurrentOwner()) {
                lock.unlock();
                lock.close();
            }
        }

    }
}
