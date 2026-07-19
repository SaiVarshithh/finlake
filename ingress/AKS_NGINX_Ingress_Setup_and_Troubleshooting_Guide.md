# AKS NGINX Ingress Setup and Troubleshooting Guide

## Objective

Expose the `finlake-airflow` application running inside AKS to the Internet using a single NGINX Ingress Controller and Azure Load Balancer.

---

# Architecture

```text
Browser
   │
   ▼
DNS (nip.io / Custom Domain)
   │
   ▼
Azure Public IP
   │
   ▼
Azure Load Balancer
   │
   ▼
NGINX Ingress Controller
   │
   ▼
Ingress Resource
   │
   ▼
ClusterIP Service
   │
   ▼
Airflow Pods
```

---

# Step 1 - Install NGINX Ingress Controller

```bash
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm repo update

helm install ingress-nginx ingress-nginx/ingress-nginx \
  --namespace ingress-nginx \
  --create-namespace
```

## Purpose

The Ingress Controller is cluster infrastructure and is installed once in its own namespace (`ingress-nginx`) so every application across namespaces can share it.

Verify:

```bash
kubectl get pods -n ingress-nginx
kubectl get svc -n ingress-nginx
```

Expected:

- Controller pod is Running.
- Service type is `LoadBalancer`.
- Azure assigns an External IP.

---

# Step 2 - Azure Creates the Load Balancer

The Service created by Helm is of type `LoadBalancer`.

Azure automatically provisions:

- Public IP
- Azure Load Balancer
- Backend Pool
- Health Probe
- Load Balancing Rules

Verify:

```bash
kubectl get svc -n ingress-nginx
```

Example:

```text
NAME                         TYPE           EXTERNAL-IP
ingress-nginx-controller     LoadBalancer   4.147.239.228
```

---

# Step 3 - Verify the Application Service

Airflow should already be deployed.

Verify:

```bash
kubectl get svc -n finlake
```

Expected:

```text
finlake-airflow   ClusterIP   8080
```

Example Service:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: finlake-airflow
  namespace: finlake
spec:
  type: ClusterIP
  selector:
    component: webserver
  ports:
  - port: 8080
    targetPort: 8080
```

The application is reachable only from inside the cluster.

---

# Step 4 - Create the Ingress Resource

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress

metadata:
  name: airflow-ingress
  namespace: finlake

spec:
  ingressClassName: nginx

  rules:
  - host: airflow.4.147.239.228.nip.io

    http:
      paths:
      - path: /
        pathType: Prefix

        backend:
          service:
            name: finlake-airflow
            port:
              number: 8080
```

Apply:

```bash
kubectl apply -f airflow-ingress.yaml
```

Verify:

```bash
kubectl get ingress -n finlake
```

---

# Step 5 - Validate Access

Open:

```text
http://airflow.4.147.239.228.nip.io
```

Expected:

- HTTP 302 redirect to `/home`
- Airflow UI loads successfully.

---

# End-to-End Request Flow

```text
Browser
    │
    ▼
airflow.<public-ip>.nip.io
    │
    ▼
Azure Public IP
    │
    ▼
Azure Load Balancer
    │
    ▼
NGINX Ingress Controller
    │
    ▼
Ingress Rule
    │
    ▼
ClusterIP Service
    │
    ▼
Airflow Pods
```

---

# Reusing for Other Applications

For MinIO, Grafana, Superset, etc.:

1. Deploy the application.
2. Expose it using a ClusterIP Service.
3. Create an Ingress pointing to that Service.
4. Reuse the existing NGINX Ingress Controller and Azure Load Balancer.

No additional LoadBalancer or Ingress Controller is required.

---

# Troubleshooting Steps

## 1. Verify Ingress Controller

```bash
kubectl get pods -n ingress-nginx
kubectl logs -n ingress-nginx deploy/ingress-nginx-controller
```

---

## 2. Verify Service

```bash
kubectl get svc -n ingress-nginx
kubectl describe svc ingress-nginx-controller -n ingress-nginx
```

Check:

- External IP assigned
- Ports 80/443 exposed

---

## 3. Timeout While Accessing Public IP

Symptom:

```
ERR_CONNECTION_TIMED_OUT
```

Verify:

```bash
curl http://<external-ip>
```

If timeout occurs, update the Service:

```bash
kubectl patch svc ingress-nginx-controller \
-n ingress-nginx \
-p '{"spec":{"externalTrafficPolicy":"Local"}}'
```

Confirm:

```bash
kubectl get svc ingress-nginx-controller \
-n ingress-nginx \
-o jsonpath="{.spec.healthCheckNodePort}"
```

A HealthCheckNodePort should be allocated.

Retry:

```bash
curl http://<external-ip>
```

Expected:

```
404 Not Found
```

A 404 confirms NGINX is reachable.

---

## 4. Verify Application Internally

```bash
kubectl exec -it <pod> -n finlake -- sh

curl -I http://finlake-airflow:8080
```

Expected:

```
HTTP/1.1 302 FOUND
```

---

## 5. Verify Ingress

```bash
kubectl get ingress -A
kubectl describe ingress airflow-ingress -n finlake
```

---

## 6. Hostname Mismatch

If the browser returns 404 but NGINX is reachable, verify:

```bash
kubectl get ingress airflow-ingress -n finlake -o yaml
```

Ensure:

```yaml
host: airflow.<external-ip>.nip.io
```

matches the current LoadBalancer IP.

Patch if required.

---

## 7. Test Host Header

```bash
curl -I -H "Host: airflow.<external-ip>.nip.io" http://<external-ip>
```

---

## 8. Final Validation

```bash
curl -I http://airflow.<external-ip>.nip.io
```

Expected:

```
HTTP/1.1 302 FOUND
Location: /home
```

The application should now be accessible from the browser.
