package site.soroosh.converter;

public class PendingRequestConverter {

    // Combining userId and pendingRequestId guarantees each key to be globally unique.
    public static String getPendingRequestMessageKey(String userId, String pendingRequestId) {
        return userId + "_" + pendingRequestId;
    }

}
