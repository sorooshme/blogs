set -e

/opt/kafka/bin/kafka-topics.sh --create --partitions 1 --if-not-exists --topic Loan.Requests --bootstrap-server broker-1:9092
/opt/kafka/bin/kafka-topics.sh --create --partitions 1 --if-not-exists --topic Serialized.Loan.Requests --bootstrap-server broker-1:9092
/opt/kafka/bin/kafka-topics.sh --create --partitions 1 --if-not-exists --topic Loan.Results --bootstrap-server broker-1:9092

/opt/kafka/bin/kafka-topics.sh --create --partitions 1 --if-not-exists --topic kafka-per-user-queue-app-pending-requests-changelog --config "cleanup.policy=compact" --bootstrap-server broker-1:9092
/opt/kafka/bin/kafka-topics.sh --create --partitions 1 --if-not-exists --topic kafka-per-user-queue-app-inflight-state-changelog --config "cleanup.policy=compact" --bootstrap-server broker-1:9092

echo "Created the topics"
