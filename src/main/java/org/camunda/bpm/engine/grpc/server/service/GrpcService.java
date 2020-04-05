package org.camunda.bpm.engine.grpc.server.service;

import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.ExternalTaskService;
import org.camunda.bpm.engine.externaltask.ExternalTaskQueryBuilder;
import org.camunda.bpm.engine.externaltask.ExternalTaskQueryTopicBuilder;
import org.camunda.bpm.engine.externaltask.LockedExternalTask;
import org.camunda.bpm.engine.grpc.CompleteRequest;
import org.camunda.bpm.engine.grpc.CompleteResponse;
import org.camunda.bpm.engine.grpc.CreateMessageRequest;
import org.camunda.bpm.engine.grpc.CreateMessageResponse;
import org.camunda.bpm.engine.grpc.ExtendLockRequest;
import org.camunda.bpm.engine.grpc.ExtendLockResponse;
import org.camunda.bpm.engine.grpc.ExternalTaskGrpc.ExternalTaskImplBase;
import org.camunda.bpm.engine.grpc.FetchAndLockRequest;
import org.camunda.bpm.engine.grpc.FetchAndLockResponse;
import org.camunda.bpm.engine.grpc.HandleBpmnErrorRequest;
import org.camunda.bpm.engine.grpc.HandleBpmnErrorResponse;
import org.camunda.bpm.engine.grpc.HandleFailureRequest;
import org.camunda.bpm.engine.grpc.HandleFailureResponse;
import org.camunda.bpm.engine.grpc.UnlockRequest;
import org.camunda.bpm.engine.grpc.UnlockResponse;
import org.camunda.bpm.engine.grpc.server.repository.ConnectionRepository;
import org.camunda.bpm.engine.impl.MessageCorrelationBuilderImpl;
import org.camunda.bpm.engine.impl.interceptor.CommandExecutor;
import org.camunda.bpm.engine.runtime.MessageCorrelationBuilder;
import org.camunda.spin.plugin.variable.value.JsonValue;
import org.camunda.spin.plugin.variable.value.impl.JsonValueImpl;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.lang.Boolean.TRUE;

@Slf4j
@Service
@RequiredArgsConstructor
public class GrpcService extends ExternalTaskImplBase {

    private final ExternalTaskService externalTaskService;

    private final ConnectionRepository connectionRepository;

    private final CommandExecutor commandExecutor;

    @Override
    public StreamObserver<FetchAndLockRequest> fetchAndLock(StreamObserver<FetchAndLockResponse> responseObserver) {
        StreamObserver<FetchAndLockRequest> requestObserver = new StreamObserver<FetchAndLockRequest>() {

            @Override
            public void onNext(FetchAndLockRequest request) {
                log.info("fetchAndLock::onNext", request);

                try {
                    informClient(request, responseObserver);
                } catch (Throwable t) {
                    log.error("fetchAndLock::onNext::informClient error", t);
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("fetchAndLock::onError", t);
            }

            @Override
            public void onCompleted() {
                log.info("fetchAndLock::onCompleted");

                connectionRepository.remove(responseObserver);
                responseObserver.onCompleted();
            }
        };
        return requestObserver;
    }

    @Override
    public void complete(CompleteRequest request, StreamObserver<CompleteResponse> responseObserver) {
        log.info("Complete external task", request);

        try {
            externalTaskService.complete(
                request.getId(),
                request.getWorkerId(),
                assembleVariables(request.getVariables())
            );

            responseObserver.onNext(CompleteResponse.newBuilder().setStatus("200").build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error on completing task " + request.getId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void handleFailure(HandleFailureRequest request, StreamObserver<HandleFailureResponse> responseObserver) {
        log.info("Handle external task failure", request);

        try {
            externalTaskService.handleFailure(
                request.getId(),
                request.getWorkerId(),
                request.getErrorMessage(),
                request.getErrorDetails(),
                request.getRetries(),
                request.getRetryTimeout()
            );

            responseObserver.onNext(HandleFailureResponse.newBuilder().setStatus("200").build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error on handling failure for task " + request.getId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void handleBpmnError(HandleBpmnErrorRequest request, StreamObserver<HandleBpmnErrorResponse> responseObserver) {
        log.info("Handle external task BPMN error", request);

        try {
            externalTaskService.handleBpmnError(
                request.getId(),
                request.getWorkerId(),
                request.getErrorCode(),
                request.getErrorMessage(),
                assembleVariables(request.getVariables())
            );

            responseObserver.onNext(HandleBpmnErrorResponse.newBuilder().setStatus("200").build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error on handling BPMN error for task " + request.getId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void unlock(UnlockRequest request, StreamObserver<UnlockResponse> responseObserver) {
        log.info("Unlock external task", request);

        try {
            externalTaskService.unlock(request.getId());
            responseObserver.onNext(UnlockResponse.newBuilder().setStatus("200").build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error on unlocking task " + request.getId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void extendLock(ExtendLockRequest request, StreamObserver<ExtendLockResponse> responseObserver) {
        log.info("Lock external task", request);

        try {
            externalTaskService.extendLock(request.getId(), request.getWorkerId(), request.getDuration());
            responseObserver.onNext(ExtendLockResponse.newBuilder().setStatus("200").build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error on extending lock for task " + request.getId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void createMessage(CreateMessageRequest request, StreamObserver<CreateMessageResponse> responseObserver) {
        try {
            log.info(String.format("Correlating message %s", request.getMessageName()));

            MessageCorrelationBuilder messageCorrelationBuilder = new MessageCorrelationBuilderImpl(commandExecutor, request.getMessageName());

            if (!request.getProcessInstanceId().isEmpty()) {
                messageCorrelationBuilder.processInstanceId(request.getProcessInstanceId());
            }

            if (!request.getVariables().isEmpty()) {
                messageCorrelationBuilder.setVariables(assembleVariables(request.getVariables()));
            }

            if (!request.getBusinessKey().isEmpty()) {
                messageCorrelationBuilder.processInstanceBusinessKey(request.getBusinessKey());
            }

            messageCorrelationBuilder.correlate();

            responseObserver.onNext(CreateMessageResponse.newBuilder().setProcessInstanceId(request.getProcessInstanceId() == null ? "" : request.getProcessInstanceId()).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error on correlating message " + request.getMessageName(), e);
            responseObserver.onError(e);
        }
    }

    private void informClient(FetchAndLockRequest request, StreamObserver<FetchAndLockResponse> client) {
        ExternalTaskQueryBuilder fetchBuilder = createQuery(request, externalTaskService);
        List<LockedExternalTask> lockedTasks = fetchBuilder.execute();

        log.info("Inform client about {} tasks", lockedTasks.size(), request);

        if (lockedTasks.isEmpty()) {
            // if no external tasks locked => save the response observer and
            // notify later when external task created for the topic in the engine
            connectionRepository.add(request, client);
        } else {
            JsonValue variables = lockedTasks.get(0).getVariables().getValueTyped("variables");

            FetchAndLockResponse reply = FetchAndLockResponse.newBuilder()
                .setId(lockedTasks.get(0).getId())
                .setWorkerId(request.getWorkerId())
                .setTopicName(lockedTasks.get(0).getTopicName())
                .setRetries(lockedTasks.get(0).getRetries() == null ? -1 : lockedTasks.get(0).getRetries())
                .setBusinessKey(lockedTasks.get(0).getBusinessKey() == null ? "" : lockedTasks.get(0).getBusinessKey())
                .setProcessInstanceId(lockedTasks.get(0).getProcessInstanceId() == null ? "" : lockedTasks.get(0).getProcessInstanceId())
                .setVariables(variables.getValueSerialized())
                .build();
            client.onNext(reply);
        }
    }

    public static ExternalTaskQueryBuilder createQuery(FetchAndLockRequest request, ExternalTaskService externalTaskService) {
        ExternalTaskQueryBuilder fetchBuilder = externalTaskService.fetchAndLock(1,
            request.getWorkerId(),
            request.getUsePriority());

        if (request.getTopicList() != null) {
            for (FetchAndLockRequest.FetchExternalTaskTopic topicDto : request.getTopicList()) {
                ExternalTaskQueryTopicBuilder topicFetchBuilder = fetchBuilder.topic(topicDto.getTopicName(), topicDto.getLockDuration());

                if (notEmpty(topicDto.getBusinessKey())) {
                    topicFetchBuilder = topicFetchBuilder.businessKey(topicDto.getBusinessKey());
                }

                if (notEmpty(topicDto.getProcessDefinitionId())) {
                    topicFetchBuilder.processDefinitionId(topicDto.getProcessDefinitionId());
                }

                if (notEmpty(topicDto.getProcessDefinitionIdInList())) {
                    topicFetchBuilder.processDefinitionIdIn(topicDto.getProcessDefinitionIdInList().toArray(new String[topicDto.getProcessDefinitionIdInList().size()]));
                }

                if (notEmpty(topicDto.getProcessDefinitionKey())) {
                    topicFetchBuilder.processDefinitionKey(topicDto.getProcessDefinitionKey());
                }

                if (notEmpty(topicDto.getProcessDefinitionKeyInList())) {
                    topicFetchBuilder.processDefinitionKeyIn(topicDto.getProcessDefinitionKeyInList().toArray(new String[topicDto.getProcessDefinitionKeyInList().size()]));
                }

                if (topicDto.getDeserializeValues()) {
                    topicFetchBuilder = topicFetchBuilder.enableCustomObjectDeserialization();
                }

                if (topicDto.getLocalVariables()) {
                    topicFetchBuilder = topicFetchBuilder.localVariables();
                }

                if (TRUE.equals(topicDto.getWithoutTenantId())) {
                    topicFetchBuilder = topicFetchBuilder.withoutTenantId();
                }

                if (notEmpty(topicDto.getTenantIdInList())) {
                    topicFetchBuilder = topicFetchBuilder.tenantIdIn(topicDto.getTenantIdInList().toArray(new String[topicDto.getTenantIdInList().size()]));
                }

                if (notEmpty(topicDto.getProcessDefinitionVersionTag())) {
                    topicFetchBuilder = topicFetchBuilder.processDefinitionVersionTag(topicDto.getProcessDefinitionVersionTag());
                }

                fetchBuilder = topicFetchBuilder;
            }
        }

        return fetchBuilder;
    }

    private static boolean notEmpty(String value) {
        return value != null && !value.isEmpty();
    }

    private static boolean notEmpty(Collection<?> list) {
        return list != null && !list.isEmpty();
    }

    private Map<String, Object> assembleVariables(String jsonValue) {
        return Collections.singletonMap("variables", new JsonValueImpl(jsonValue).getValue());
    }

}
