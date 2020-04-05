package org.camunda.bpm.engine.grpc.server.repository;

import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.tuple.Pair;
import org.camunda.bpm.engine.grpc.FetchAndLockRequest;
import org.camunda.bpm.engine.grpc.FetchAndLockResponse;

import java.util.List;

public interface ConnectionRepository {

    void add(FetchAndLockRequest request, StreamObserver<FetchAndLockResponse> streamObserver);

    void remove(StreamObserver<FetchAndLockResponse> streamObserver);

    void clear();

    List<Pair<FetchAndLockRequest, StreamObserver<FetchAndLockResponse>>> get();
}
