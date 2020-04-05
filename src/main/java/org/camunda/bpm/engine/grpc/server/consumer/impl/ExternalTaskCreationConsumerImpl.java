package org.camunda.bpm.engine.grpc.server.consumer.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.grpc.server.consumer.ExternalTaskCreationConsumer;
import org.camunda.bpm.engine.grpc.server.informer.ExternalTaskInformer;
import org.camunda.bpm.engine.impl.ProcessEngineImpl;
import org.camunda.bpm.engine.impl.util.SingleConsumerCondition;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ExternalTaskCreationConsumerImpl implements ExternalTaskCreationConsumer {

    private final ExternalTaskInformer externalTaskInformer;

    private boolean isRunning = false;

    private Thread handlerThread = new Thread(this);

    private SingleConsumerCondition condition = new SingleConsumerCondition(handlerThread);

    @Override
    public void start() throws AlreadyStartedException {
        if (isRunning) {
            throw new AlreadyStartedException();
        }

        isRunning = true;
        handlerThread.start();

        ProcessEngineImpl.EXT_TASK_CONDITIONS.addConsumer(condition);
    }

    @Override
    public void stop() throws AlreadyStopedException {
        if (!isRunning) {
            throw new AlreadyStopedException();
        }

        try {
            ProcessEngineImpl.EXT_TASK_CONDITIONS.removeConsumer(condition);
        } finally {
            isRunning = false;
            condition.signal();
        }

        try {
            handlerThread.join();
        } catch (InterruptedException e) {
            log.warn("Shutting down the handler thread failed", e);
        }
    }

    @Override
    public void run() {
        do {
            try {
                log.info("Run external task listener...");
                externalTaskInformer.inform();

                log.info("Let external task listener wait...");
                condition.await(5000L);
                log.info("External task listener woke up!");
            } catch (Exception e) {
                log.error("External task listener exception", e);
            } finally {
                if (handlerThread.isInterrupted()) {
                    Thread.currentThread().interrupt();
                }
            }
        } while (isRunning);
    }
}
