package org.camunda.bpm.engine.grpc.server.consumer;

public interface ExternalTaskCreationConsumer extends Runnable {

    void start() throws AlreadyStartedException;

    void stop() throws AlreadyStopedException;

    class StateChangeException extends Exception {
    }

    class AlreadyStartedException extends StateChangeException {
    }

    class AlreadyStopedException extends StateChangeException {
    }
}
