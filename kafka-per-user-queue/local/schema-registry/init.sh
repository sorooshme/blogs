set -e

apk add --no-cache jq

curl -H "Content-Type: application/json" -XPOST http://schema-registry:8081/subjects/Loan.Requests-value/versions -d "{\"schema\": $(cat /app/loan-request.avsc | jq -c 'tojson')}"
curl -H "Content-Type: application/json" -XPOST http://schema-registry:8081/subjects/Serialized.Loan.Requests-value/versions -d "{\"schema\": $(cat /app/loan-request.avsc | jq -c 'tojson')}"
curl -H "Content-Type: application/json" -XPOST http://schema-registry:8081/subjects/Loan.Results-value/versions -d "{\"schema\": $(cat /app/loan-result.avsc | jq -c 'tojson')}"

curl -H "Content-Type: application/json" -XPOST http://schema-registry:8081/subjects/kafka-per-user-queue-app-pending-requests-changelog-value/versions -d "{\"schema\": $(cat /app/loan-request.avsc | jq -c 'tojson')}"
curl -H "Content-Type: application/json" -XPOST http://schema-registry:8081/subjects/kafka-per-user-queue-app-inflight-state-changelog-value/versions -d "{\"schema\": $(cat /app/inflight-state.avsc | jq -c 'tojson')}"

echo "Created the schemas"
