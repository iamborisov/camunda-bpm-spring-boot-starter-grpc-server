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
import org.camunda.bpm.engine.grpc.server.repository.ConnectionRepository;
import org.camunda.bpm.engine.grpc.server.service.GrpcService;
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
        log.info("Found {} pending connections", connectionRepository.get().size());

        for (Iterator<Pair<FetchAndLockRequest, StreamObserver<FetchAndLockResponse>>> iterator = connectionRepository.get().iterator(); iterator.hasNext(); ) {
            Pair<FetchAndLockRequest, StreamObserver<FetchAndLockResponse>> pair = iterator.next();
            List<LockedExternalTask> lockedTasks = GrpcService.createQuery(pair.getLeft(), externalTaskService).execute();

            lockedTasks.forEach(lockedExternalTask -> {
                log.info("Inform client about locked external task with id={}", lockedExternalTask.getId());

                pair.getRight().onNext(
                    buildResponse(lockedExternalTask)
                );
            });
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
            .setVariables(variables.getValueSerialized())
            .build();
    }
}
