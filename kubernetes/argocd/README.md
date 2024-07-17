Setting up Argo CD involves several steps. Argo CD is a declarative, GitOps continuous delivery tool for Kubernetes. Here’s a general guide to get you started:

### Prerequisites
1. **Kubernetes Cluster**: Ensure you have a running Kubernetes cluster.
2. **kubectl**: Install `kubectl` and configure it to communicate with your Kubernetes cluster.
3. **Helm**: Install Helm if you prefer using Helm charts for installation.

### Step-by-Step Guide

#### 1. Install Argo CD

You can install Argo CD using either `kubectl` or `Helm`.

**Using kubectl:**
```bash
# Create a namespace for Argo CD
kubectl create namespace argocd

# Apply the Argo CD manifests
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
```

**Using Helm:**
```bash
# Add the Argo CD Helm repo
helm repo add argo https://argoproj.github.io/argo-helm

# Update the Helm repo
helm repo update

# Install Argo CD
helm install argocd argo/argo-cd --namespace argocd --create-namespace
```

#### 2. Access the Argo CD API Server

By default, the Argo CD API server is not exposed with an external IP. You can access it using port forwarding.

```bash
kubectl port-forward svc/argocd-server -n argocd 8080:443
```

Now you can access the Argo CD web UI at `https://localhost:8080`.

#### 3. Get the Initial Admin Password

The initial password for the `admin` account is auto-generated and stored in a Kubernetes secret.

```bash
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d
```

#### 4. Login to Argo CD

Use the `argocd` CLI to login or use the web UI.

**Using the CLI:**
```bash
# Install the Argo CD CLI
VERSION=$(curl --silent "https://api.github.com/repos/argoproj/argo-cd/releases/latest" | grep -Po '"tag_name": "\K.*?(?=")')
curl -sSL -o /usr/local/bin/argocd "https://github.com/argoproj/argo-cd/releases/download/${VERSION}/argocd-linux-amd64"
chmod +x /usr/local/bin/argocd

# Login to Argo CD
argocd login localhost:8080
```

When prompted, use `admin` as the username and the password you retrieved earlier.

#### 5. Change the Admin Password

For security reasons, change the default password immediately.

```bash
argocd account update-password
```

#### 6. Connect a Git Repository

Create a new application in Argo CD by connecting it to a Git repository.

**Using the CLI:**
```bash
argocd app create <app-name> \
    --repo <repository-url> \
    --path <app-path> \
    --dest-server https://kubernetes.default.svc \
    --dest-namespace <namespace>
```

**Using the Web UI:**
1. Go to the Argo CD web UI.
2. Click on `+ New App`.
3. Fill in the required details (Application name, Project, Sync Policy, Repository URL, Revision, Path, Cluster URL, and Namespace).
4. Click `Create`.

### Additional Resources

- [Argo CD Documentation](https://argo-cd.readthedocs.io/en/stable/)
- [Argo CD GitHub Repository](https://github.com/argoproj/argo-cd)

This should get you started with Argo CD. If you encounter any issues or have specific requirements, feel free to ask!