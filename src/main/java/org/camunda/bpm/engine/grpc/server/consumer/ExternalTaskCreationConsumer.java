package org.camunda.bpm.engine.grpc.server.consumer;

public interface ExternalTaskCreationConsumer extends Runnable {

    void start() throws AlreadyStartedException;

    void stop() throws AlreadyStoppedException;

    class StateChangeException extends Exception {
    }

    class AlreadyStartedException extends StateChangeException {
    }

    class AlreadyStoppedException extends StateChangeException {
    }
}
