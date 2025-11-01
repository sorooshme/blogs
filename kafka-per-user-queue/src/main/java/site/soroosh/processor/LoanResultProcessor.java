package site.soroosh.processor;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import site.soroosh.AppProps;
import site.soroosh.converter.PendingRequestConverter;
import site.soroosh.schema.InflightState;
import site.soroosh.schema.LoanRequest;
import site.soroosh.schema.LoanResult;

import java.util.List;

public class LoanResultProcessor implements Processor<String, LoanResult, String, LoanRequest> {
    private KeyValueStore<String, InflightState> inflightStateStore;
    private KeyValueStore<String, LoanRequest> pendingRequestsStore;
    private ProcessorContext<String, LoanRequest> ctx;

    @Override
    public void init(ProcessorContext<String, LoanRequest> context) {
        ctx = context;
        inflightStateStore = context.getStateStore(AppProps.INFLIGHT_STATE_STORE_NAME);
        pendingRequestsStore = context.getStateStore(AppProps.PENDING_REQUESTS_STORE_NAME);
    }

    @Override
    public void process(Record<String, LoanResult> incomingRecord) {
        String userId = incomingRecord.key();
        LoanResult incomingResult = incomingRecord.value();
        String resultRequestId = incomingResult.getRequestId().toString();

        InflightState currentInflightState = inflightStateStore.get(userId);

        if (currentInflightState == null) {
            // This happens when:
            // 1. Message in Result did not go through the "serialized" flow
            // 2. Result topic has duplicate messages
            // In either case, we should ignore the message.

            return;
        }

        String inflightRequestId = currentInflightState.getInflightRequestId().toString();

        boolean isInflightRequest = resultRequestId.equals(inflightRequestId);

        if (!isInflightRequest) {
            // This happens when:
            // 1. Result topic has duplicate messages and this message was previously considered as "inflight"
            // We should ignore it.

            return;
        }

        // The incoming result is considered inflight
        // We must clear the state and in case any other request is pending, we should start processing them.

        List<CharSequence> pendingRequests = currentInflightState.getPendingRequestIds();

        if (pendingRequests.isEmpty()) {
            // No request is waiting, we can cleanup the inflight state

            inflightStateStore.delete(userId);

            return;
        }

        String nextRequestId = pendingRequests.removeFirst().toString(); // Update in place
        LoanRequest nextRequest = pendingRequestsStore.get(
                PendingRequestConverter.getPendingRequestMessageKey(userId, nextRequestId)
        );

        if (nextRequest == null) {
            // This is theoretically impossible, but we check it just in case.
            throw new Error(
                    "nextRequestId exists but nextRequest is empty, state store is most likely corrupted or manually modified."
            );
        }

        currentInflightState.setInflightRequestId(nextRequestId);

        inflightStateStore.put(userId, currentInflightState);
        pendingRequestsStore.delete(
                PendingRequestConverter.getPendingRequestMessageKey(userId, nextRequestId)
        ); // Delete the request because it's no longer pending

        ctx.forward(incomingRecord.withValue(nextRequest));
    }
}