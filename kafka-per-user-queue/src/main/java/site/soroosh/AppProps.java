package site.soroosh;

public class AppProps {

    public static String INFLIGHT_STATE_STORE_NAME = "inflight-state";
    public static String PENDING_REQUESTS_STORE_NAME = "pending-requests";

    public static String LOAN_REQUESTS_TOPIC = "Loan.Requests";
    public static String SERIALIZED_LOAN_REQUESTS_TOPIC = "Serialized.Loan.Requests";
    public static String LOAN_RESULTS_TOPIC = "Loan.Results";

    public static String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    public static String BOOTSTRAP_URL = "localhost:19092";
    public static String APPLICATION_ID = "kafka-per-user-queue-app";

}
