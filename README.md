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

### 1. AKS setting
In this project, we used student plan(https://portal.azure.com/).


## Author
This project is created and maintained by [@](https://github.com/LeungKwokFan)LeungKwokFan and [@](https://github.com/ipton17
)ipton17.
