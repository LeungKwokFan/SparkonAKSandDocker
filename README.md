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
5. other file: dependencies.

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

### 2. set the shell of AKS

Click the shell icon of the AKS after creating the AKS successfully.
And choose:
▪ No storage - temporary session: all the files will be gone after the session
▪ Mount storage - the files will be persisted, and you can reuse them in future session

Type this command:
'''
az aks get-credentials --resource-group <gp_name> --name <clus_name>
'''
Then the folloing message will be shown:
'''
Merged "<clus_name>" as current context in /home/<user>/.kube/config
'''


## Author
This project is created and maintained by [@](https://github.com/LeungKwokFan)**LeungKwokFan** and [@](https://github.com/ipton17
)**ipton17**.
