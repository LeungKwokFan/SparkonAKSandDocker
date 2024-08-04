# EDAonAKSandDocker

## Intro
Using Docker and Azure AKS to finish the EDA or other program. And the py. file and .ipynb file can be changed for dealing with other tasks.

## File Statement
### aks
The file ass4 in aks is for aks environment and only show the questions and corresponding answer when executed.
1. bank-eda-aks.py: how the questions and corresponding answer in aks.
2. aks-dockerfile: to build the image for submission to aks.
3. submit-AKS-template.sh: command for aks submission.
4. bank-test: a basic output result shown on the video.
5. other file: dependencies.

### local 
The file ass4test in local is for local environment. It built an image including Jupyter Lab and Spark UI for testing the ipynb file. 
1. bank-eda-test.ipynb: EDA task on dataset.
2. band-eda.py: could be submitted in local and then run.
3. submit-project.sh: command for local submission.
4. local-aks-dockerfile: to build the image including Jupyter Lab and Spark UI.
5. Dockerfile: to build the image for submission of aks.
6. other file: dependencies.

### output_file_set
contains the dataframe since we used df to figure out the problems.

### bank.csv
The dataset we selected. More detail can be found in our report or bank-eda-test.ipynb.

## Setting of the project

### 1. K8s Cluster setting

In this project, we used student plan(https://portal.azure.com/) to set the configuration. If you don not have one, you can use pay-as-u-go plan.

Create a aks, set the configuration:
* type the name and group
* set "Cluster preset configuration" as “Dev/Test"
* set "cluster region" as East Asia(Hong Kong SAR), it depends on where you are
* set "Availability zones" as None
* select Free tier for AKS Pricing
* set the node, at least 3 CPU cores and 4GB memory(D4s v3), and set min node count and max node count to 1
* Unclick Prometheus
  

### 2. Set the shell of AKS

Click the shell icon of the AKS after creating the AKS successfully.
And choose:
* No storage - temporary session: all the files will be gone after the session
* Mount storage - the files will be persisted, and you can reuse them in future session

Type this command:
```
az aks get-credentials --resource-group <gp_name> --name <clus_name>
```
Then the folloing message will be shown:
```
Merged "<clus_name>" as current context in /home/<user>/.kube/config
```

### 3. Create a Docker Hub account

Remember the name and psw.


### 4. Create a new Storage account in Azure

Click the storage account.

Under the "Basics" tab of "Create a storage account": 
* Select the Subscription and Resource group
* Fill in the Storage account name
* Region: Select "East Asia"
* Performance: Select “Standard"
* Redundancy: Select "Locally-redundant storage (LRS)
* Then press "Next: Advanced >" button to go to the next tab

Under the "Advanced" tab:
* Tick "Enable hierarchical namespace"
* Accept the other settings
* Press "Next: Networking >" to continue

Under the "Networking" tab:
* Under Network access, select “Enable public access from selected virtual networks and IP addresses”
* Selecting this allow the AKS cluster and external access to this storage account


Under the "Data protection" tab:
* Untick "Enable soft delete for blobs"(Soft delete allows user to undelete blobs if they are accidentally deleted within 7 days)

Then create one.

Next, enter the page og this storage account, and select "Networking" from the bar.
* Select “+ Add existing virtual network"
* Tick "Select all“ twice to select the virtual network and the subnet, and
* Click "Add" at the bottom
* This allows the AKS cluster to access the storage account
* Under Firewall, add your host computer's IP address and/or your organization subnet address
* This allows your host computer or machines in your organization to access and view the contents of the storage account, and to upload / download / delete files
* Click "Save" to save the changes


### 5. Create a blob container

* Go back to the storage accounts main page 
* Click on the storage account you just created 
* Click "Container"
* Click "+ Container"
* Fill in a unique name for the blob container, and click "Create“

And this container is for:
* uploading data files for processing by Spark programs,
* uploading the Spark program files by spark-submit, and
* downloading data files that are generated by the Spark programs


### 6. Create the image for running on aks

* open the terminal, type
```
docker build –t <username>/<image_name> -f aks-dockerfile .
```
Using `aks/aks-dockerfile`, and the info of your hub account.
* Next, login to your Docker Hub account
```
docker login -u <username>
```
* Then, push the image to your public repository
```
docker push <username>/<image_name>
```

In this project, **ipton17** created the image used on AKS, you can find it on https://hub.docker.com/r/ipton17/ass4aks.


### 7. Create first local image for submission of aks

* Build the image,
```
docker build –t <image_name> -f Dockerfile .
```
* Then run the image,
```
docker run --name <container_name> -it -v <host_folder>:/home/spark/mount <image_name>
```

You can use 2 ways to open the corresponding terminal:
1. Run ```docker exec –it <container_name> /bin/bash``` on your local terminal;
2. Open the terminal on Docker Desktop by clicking the options of corresponding container.

* Open the terminal, type:
```
az login --use-device-code
```
Then you will get:
```
To sign in, use a web browser to open the page https://microsoft.com/ devicelogin and enter the code PASSWORD to authenticate.
```
Then open https://microsoft.com and enter PASSWORD.

If success, enter ```az aks list``` and you will get a long message including ```"azurePortalFqdn": "<<<Your AKS API server address>>>"```

* Then, we connect this container and our Azure aks, we enter:
```
az aks get-credentials --resource-group <resrc_grp_name> --name <clus_name>
```
And see:
```
Merged "<clus_name>" as current context in /home/spark/.kube/config
```

Then, we enter:
```
kubectl config current-context
```
And will see the AKS cluster name.


### 8. Create secon local image for testing

* Build the image,
```
docker build –t <image_name> -f local-aks-dockerfile .
```
* Then run the image,
```
docker run --name <container_name> -it -v <host_folder>:/home/spark/mount <image_name>
```





## Author
This project is created and maintained by [@](https://github.com/LeungKwokFan)**LeungKwokFan** and [@](https://github.com/ipton17
)**ipton17**.
