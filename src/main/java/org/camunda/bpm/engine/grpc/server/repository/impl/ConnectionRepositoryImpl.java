package org.camunda.bpm.engine.grpc.server.repository.impl;

import io.grpc.stub.StreamObserver;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.camunda.bpm.engine.grpc.FetchAndLockRequest;
import org.camunda.bpm.engine.grpc.FetchAndLockResponse;
import org.camunda.bpm.engine.grpc.server.repository.ConnectionRepository;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
@NoArgsConstructor
public class ConnectionRepositoryImpl implements ConnectionRepository {

    private List<Pair<FetchAndLockRequest, StreamObserver<FetchAndLockResponse>>> connections = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void add(FetchAndLockRequest request, StreamObserver<FetchAndLockResponse> streamObserver) {
        connections.add(Pair.of(request, streamObserver));
    }

    @Override
    public void remove(StreamObserver<FetchAndLockResponse> streamObserver) {
        connections.removeIf(pair -> pair.getRight().equals(streamObserver));
    }

    @Override
    public void clear() {
        connections.clear();
    }

    @Override
    public List<Pair<FetchAndLockRequest, StreamObserver<FetchAndLockResponse>>> get() {
        return connections;
    }
}
