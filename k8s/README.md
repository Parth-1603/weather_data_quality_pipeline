# Optional Kubernetes extension (Minikube)

Kubernetes is not needed for the basic laptop assignment. This optional extension runs the pipeline as a Kubernetes CronJob. A local image must be built and available to the cluster.

## 1. Install prerequisites
Install Docker Desktop and Minikube, then start a local cluster:
```powershell
minikube start
minikube image build -t weather-pipeline:1.0 .
```

## 2. Apply the manifest
```powershell
kubectl apply -f k8s/weather-pipeline.yaml
kubectl get cronjobs
kubectl get jobs
kubectl get pods
```

The CronJob runs on a schedule. For an immediate test:
```powershell
kubectl create job --from=cronjob/weather-pipeline weather-pipeline-manual
kubectl logs job/weather-pipeline-manual
```

## Persistence caveat
This starter CronJob writes to the container filesystem. When a pod is removed, files inside it may be lost. For a proper deployment, add a PersistentVolumeClaim and mount it at `/app/data` and `/app/reports`, or write outputs to object storage. A PVC is cluster-specific, so configure storage based on your chosen Kubernetes environment.
