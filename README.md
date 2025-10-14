# End-to-End-Deep-Learning-Project-1
End-to-End Deep Learning Project - 1 - Chicken Disease Classification 

### Tools you have to install:-

1. Anaconda: https://www.anaconda.com/
2. VS Code: https://code.visualstudio.com/
3. Git: https://git-scm.com/

### For Flowchart 

- https://whimsical.com/a

### Project Management

- https://www.atlassian.com/software/jira

## Workflows

1. Update config.yaml
3. Update params.yaml
4. Update the entity
5. Update the configuration manager in src config
6. Update the components
7. Update the pipeline 
8. Update the main.py
9. Update the app.py

## How to run?

```bash
git clone https://github.com/tanjinadnanabir/End-to-End-Deep-Learning-Project-1
```

```bash
conda create -n deep-learning python=3.12.4 -y
```

```bash
conda activate deep-learning
```

```bash
pip install -r requirements.txt
```

```bash
git add .
git status
git commit -m "message"
git push origin branch
```

### Export the environment

```bash
export AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY_ID""
export AWS_SECRET_ACCESS_KEY="YOUR_SECRET_ACCESS_KEY"
```
### Finally run the following command

```bash
python app.py
```

```bash
open up you local host and port
```

# AWS-CICD-Deployment-with-Github-Actions

## 1. Login to AWS console.

## 2. Create IAM user for deployment

	With specific access

	1. EC2 access : It is virtual machine

	2. ECR: Elastic Container registry to save your docker image in aws

	Description: About the deployment

	1. Build docker image of the source code

	2. Push your docker image to ECR

	3. Launch Your EC2 

	4. Pull Your image from ECR in EC2

	5. Lauch your docker image in EC2

	Policy:

	1. AmazonEC2ContainerRegistryFullAccess

	2. AmazonEC2FullAccess
	
## 3. Create ECR repo to store/save docker image

    - Save the URI: 
	
## 4. Create EC2 machine (Ubuntu) 

## 5. Open EC2 and Install docker in EC2 Machine:
	
	Optinal

	sudo apt-get update -y

	sudo apt-get upgrade
	
	Required

	curl -fsSL https://get.docker.com -o get-docker.sh

	sudo sh get-docker.sh

	sudo usermod -aG docker ubuntu

	newgrp docker
	
# 6. Configure EC2 as self-hosted runner:

    setting > actions > runner > new self hosted runner > choose os > then run command one by one

# 7. Setup github secrets:

    AWS_ACCESS_KEY_ID=

    AWS_SECRET_ACCESS_KEY=

    AWS_REGION = us-east-1

    AWS_ECR_LOGIN_URI =  

    ECR_REPOSITORY_NAME = 
