# Deploy Airflow with Helm chart

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

8. Start using Airflow:

    You can now start using Airflow to create and manage your workflows. Refer to the Airflow documentation for more information on how to use Airflow.

Remember to clean up the deployment when you're done:

```shell
helm uninstall airflow
```




## Config CI/CD With Github Action and Git-Sync

