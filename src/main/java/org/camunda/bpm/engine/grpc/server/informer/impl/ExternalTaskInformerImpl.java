package org.camunda.bpm.engine.grpc.server.informer.impl;

import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.externaltask.LockedExternalTask;
import org.camunda.bpm.engine.grpc.FetchAndLockRequest;
import org.camunda.bpm.engine.grpc.FetchAndLockResponse;
import org.camunda.bpm.engine.grpc.server.informer.ExternalTaskInformer;
import org.camunda.bpm.engine.grpc.server.informer.ExternalTaskQuery;
import org.camunda.bpm.engine.grpc.server.repository.ConnectionRepository;
import org.camunda.spin.plugin.variable.value.JsonValue;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ExternalTaskInformerImpl implements ExternalTaskInformer {

    private final ConnectionRepository connectionRepository;

    private final ExternalTaskService externalTaskService;

    @Override
    public void inform() {
        List<Pair<FetchAndLockRequest, StreamObserver<FetchAndLockResponse>>> connections = connectionRepository.get();
        log.info("Found {} pending connections", connections.size());

        for (Iterator<Pair<FetchAndLockRequest, StreamObserver<FetchAndLockResponse>>> iterator = connections.iterator(); iterator.hasNext(); ) {
            Pair<FetchAndLockRequest, StreamObserver<FetchAndLockResponse>> pair = iterator.next();

            List<LockedExternalTask> tasks;

            do {
                tasks = ExternalTaskQuery.create(pair.getLeft(), externalTaskService).execute();

                tasks.forEach(lockedExternalTask -> {
                    log.info("Inform client about locked external task with id={}", lockedExternalTask.getId());

                    pair.getRight().onNext(
                        buildResponse(lockedExternalTask)
                    );
                });
            } while (tasks.size() > 0);
        }
    }

    private FetchAndLockResponse buildResponse(LockedExternalTask lockedExternalTask) {
        JsonValue variables = lockedExternalTask.getVariables().getValueTyped("variables");

        return FetchAndLockResponse.newBuilder()
            .setId(lockedExternalTask.getId())
            .setWorkerId(lockedExternalTask.getWorkerId())
            .setTopicName(lockedExternalTask.getTopicName())
            .setRetries(lockedExternalTask.getRetries() == null ? -1 : lockedExternalTask.getRetries())
            .setBusinessKey(lockedExternalTask.getBusinessKey() == null ? "" : lockedExternalTask.getBusinessKey())
            .setProcessInstanceId(lockedExternalTask.getProcessInstanceId())
            .setVariables(variables == null ? "" : variables.getValueSerialized())
            .build();
    }
}
