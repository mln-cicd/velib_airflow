

## HELM CHARTS
HELM
```sh

curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod 700 get_helm.sh
./get_helm.sh
helm version
```


ARGO WORKFLOWS

```sh
helm repo add argo https://argoproj.github.io/argo-helm
helm repo update

helm install my-release argo/argo-workflows --set server.authMode=""

kubectl logs -n default -l app.kubernetes.io/name=argo-workflows-server
kubectl describe pod -n default -l app.kubernetes.io/name=argo-workflows-server
kubectl get pods -n default -l app.kubernetes.io/name=argo-workflows-server


kubectl port-forward service/my-release-argo-workflows-server 2746:2746






```

MINIO
```sh
helm repo add minio-operator https://operator.min.io

helm install \
  --namespace minio-operator \
  --create-namespace \
  operator minio-operator/operator

kubectl get all -n minio-operator

kubectl get secret/console-sa-secret -n minio-operator -o json | jq -r ".data.token" | base64 -d

kubectl port-forward svc/console -n minio-operator 9090:9090


```

eyJhbGciOiJSUzI1NiIsImtpZCI6ImhNcXVxejZPNXpkQ3RxUU1sdEhocGFfSUtfQzdKcHpYOF9VUWFCOXI5MEUifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJtaW5pby1vcGVyYXRvciIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJjb25zb2xlLXNhLXNlY3JldCIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50Lm5hbWUiOiJjb25zb2xlLXNhIiwia3ViZXJuZXRlcy5pby9zZ