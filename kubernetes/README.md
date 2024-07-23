# Deploy Airflow on k8s
## Deploy Airflow with Helm chart (Native)

Load Airflow DAGs
To deploy Airflow on Kubernetes using Helm chart, follow these steps:
## Deploy
1. Create Namespace airflow:
    ```shell
    kubectl create namespace airflow && kubectl config set-context --current --namespace=airflow
    ```

2. Add the Airflow Helm repository to Helm:

    ```shell
    helm repo add apache-airflow https://airflow.apache.org
    ```

3. Update the Helm repositories:

    ```shell
    helm repo update
    ```

4. Install Airflow using the Helm chart:

    ```shell
    helm upgrade --install airflow apache-airflow/airflow --namespace airflow -f values.yaml 
    ```

    This command will install Airflow with the default configuration. You can customize the installation by providing a values file or using Helm chart options.

5. Monitor the deployment:

    ```shell
    kubectl get pods
    ```

    This command will show the status of the Airflow pods. Wait until all the pods are in the "Running" state.

6. Access the Airflow UI:

    ```shell
    kubectl port-forward svc/airflow-web 8080:8080
    ```

    This command will forward the Airflow UI port to your local machine. You can access the Airflow UI by opening http://localhost:8080 in your web browser.

7. Configure Airflow:

    Airflow can be configured using the `values.yaml` file or by providing custom configuration files. Refer to the Airflow documentation for more information on how to configure Airflow.
    ```yaml
    executor: "KubernetesExecutor"
    image:
    airflow:
        repository: vantuan12345/airlake:v1.0.0
        tag: latest
        ## WARNING: even if set to "Always" DO NOT reuse tag names, 
        ##          containers only pull the latest image when restarting
        pullPolicy: IfNotPresent

    gitSync:
    enabled: true
        
        ## NOTE: some git providers will need an `ssh://` prefix
    repo: "git@github.com:tuancamtbtx/airflow-on-k8s.git"
    branch: "master"
        
        ## the sub-path within your repo where dags are located
        ## NOTE: airflow will only see dags under this path, but the whole repo will still be synced
    subPath: "airflow_dags/dags"
        
        ## number of seconds to wait between syncs
        ## NOTE: also sets `AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL` unless overwritten in `airflow.config`
    syncWait: 60
    sshKeySecret: airflow-ssh-secret
    ```
8. Start using Airflow:

    You can now start using Airflow to create and manage your workflows. Refer to the Airflow documentation for more information on how to use Airflow.

Remember to clean up the deployment when you're done:

```shell
helm uninstall airflow
```



## Deploy Airflow with Helm chart (ArgoCD)


Deploying Apache Airflow using Helm charts with ArgoCD involves several steps. Below is a high-level guide to help you through the process.

### Prerequisites
1. **Kubernetes Cluster**: Ensure you have a running Kubernetes cluster.
2. **kubectl**: Command-line tool for Kubernetes.
3. **Helm**: Package manager for Kubernetes.
4. **ArgoCD**: GitOps continuous delivery tool for Kubernetes.

### Step-by-Step Guide

#### 1. Install Helm
If you haven't installed Helm yet, you can do so by following the instructions on the [Helm installation page](https://helm.sh/docs/intro/install/).

#### 2. Install ArgoCD
If ArgoCD is not already installed in your cluster, follow these steps:

```sh
# Install ArgoCD namespace
kubectl create namespace argocd

# Install ArgoCD
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
```

#### 3. Access ArgoCD UI
To access the ArgoCD UI, you need to port-forward the ArgoCD server service:

```sh
kubectl port-forward svc/argocd-server -n argocd 8080:443
```

Then, open your browser and go to `https://localhost:8080`.

#### 4. Log in to ArgoCD
Get the initial password for the `admin` user:

```sh
kubectl get pods -n argocd
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d; echo
```

Use `admin` as the username and the password retrieved above to log in.

#### 5. Add Helm Repository
Add the Airflow Helm chart repository:

```sh
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

#### 6. Create a Git Repository for ArgoCD
Create a Git repository that will contain your Helm chart configuration. This repository will be used by ArgoCD to sync the state of your Airflow deployment.

#### 7. Create a Helm Values File
In your Git repository, create a values file for the Airflow Helm chart (e.g., `values.yaml`). Customize it according to your needs.

#### 8. Create an ArgoCD Application
Create an ArgoCD Application manifest (e.g., `airflow.yaml`) in your Git repository:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: airflow
  namespace: argocd
spec:
  destination:
    namespace: default  # Change to your desired namespace
    server: 'https://kubernetes.default.svc'
  project: default
  source:
    path: ./kubernetes/argocd/airflow  # Path to your Helm chart in the repository
    repoURL: 'https://github.com/tuancamtbtx/airflow-on-k8s'  # Replace with your repo URL
    targetRevision: HEAD
    helm:
      valueFiles:
        - values.yaml
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
```

#### 9. Push Changes to Git Repository
Push the `values.yaml` and `airflow.yaml` to your Git repository.

#### 10. Apply the ArgoCD Application
Apply the ArgoCD Application manifest:

```sh
kubectl apply -f https://github.com/tuancamtbtx/airflow-on-k8s/kubernetes/argocd/airflow/airflow-app.yaml -n argocd
```

#### 11. Monitor Deployment
Go to the ArgoCD UI and monitor the deployment of your Airflow instance. ArgoCD will sync the state of your Kubernetes cluster with the configuration in your Git repository.

### Summary
You have now deployed Apache Airflow using Helm charts with ArgoCD. This setup allows you to manage your Airflow deployment using GitOps principles, ensuring that your cluster state is always in sync with your Git repository.