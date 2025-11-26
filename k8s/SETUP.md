## Deploy
cd k8s
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f zookeeper.yaml
kubectl apply -f kafka.yaml
kubectl apply -f kafka-ui.yaml
kubectl apply -f flink.yaml

## Check Status
kubectl get all -n stock-analytics
kubectl get pods -n stock-analytics
kubectl get svc -n stock-analytics

## Access Services
- Kafka: localhost:30092
- Kafka UI: http://localhost:30080
- Flink: http://localhost:30081

## Logs
kubectl logs -f deployment/kafka -n stock-analytics
kubectl logs -f deployment/flink-jobmanager -n stock-analytics

## Cleanup
kubectl delete namespace stock-analytics

