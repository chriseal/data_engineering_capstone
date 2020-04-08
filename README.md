# Data Engineering Capstone Project

## Overview

The purpose of the data engineering capstone project is to give you a chance to combine what you've learned throughout the program. This project will be an important part of your portfolio that will help you achieve your data engineering-related career goals.

In this project, you can choose to complete the project provided for you, or define the scope and data for a project of your own design. Either way, you'll be expected to go through the same steps outlined below.

## Udacity Provided Project

In the Udacity provided project, you'll work with four datasets to complete the project. The main dataset will include data on immigration to the United States, and supplementary datasets will include data on airport codes, U.S. city demographics, and temperature data. You're also welcome to enrich the project with additional data if you'd like to set your project apart.

## Datasets

- The following datasets are included in the project workspace. We purposely did not include a lot of detail about the data and instead point you to the sources. This is to help you get experience doing a self-guided project and researching the data yourself. If something about the data is unclear, make an assumption, document it, and move on. Feel free to enrich your project by gathering and including additional data sources.
  - *I94 Immigration Data*: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. This is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project.
  - *World Temperature Data*: This dataset came from Kaggle. You can read more about it here.
  - *U.S. City Demographic Data*: This data comes from OpenSoft. You can read more about it here.
  - *Airport Code Table*: This is a simple table of airport codes and corresponding cities. It comes from here.

### Instructions

#### Save all datasets to S3

- Download the world temperature dataset from the provided link above and upload it to the Udacity workspace
- Save all files to S3 bucket by calling `utils.load_raw_files_to_s3()`

To help guide your project, we've broken it down into a series of steps.

#### Step 1: Scope the Project and Gather Data

- Since the scope of the project will be highly dependent on the data, these two things happen simultaneously. In this step, you’ll:
  - Identify and gather the data you'll be using for your project (at least two sources and more than 1 million rows). See Project Resources for ideas of what data you can use.
  - Explain what end use cases you'd like to prepare the data for (e.g., analytics table, app back-end, source-of-truth database, etc.)

#### Step 2: Explore and Assess the Data

- Explore the data to identify data quality issues, like missing values, duplicate data, etc.
- Document steps necessary to clean the data

#### Step 3: Define the Data Model

- Map out the conceptual data model and explain why you chose that model
- List the steps necessary to pipeline the data into the chosen data model

#### Step 4: Run ETL to Model the Data

- Create the data pipelines and the data model
- Include a data dictionary
- Run data quality checks to ensure the pipeline ran as expected
  - Integrity constraints on the relational database (e.g., unique key, data type, etc.)
  - Unit tests for the scripts to ensure they are doing the right thing
  - Source/count checks to ensure completeness

#### Step 5: Complete Project Write Up

What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?

- Clearly state the rationale for the choice of tools and technologies for the project.
- Document the steps of the process.
- Propose how often the data should be updated and why.
- Post your write-up and final data model in a GitHub repo.
- Include a description of how you would approach the problem differently under the following scenarios:
  - If the data was increased by 100x.
  - If the pipelines were run on a daily basis by 7am.
  - If the database needed to be accessed by 100+ people.


## Rubric

In the Project Rubric, you'll see more detail about the requirements. Use the rubric to assess your own project before you submit to Udacity for review. As with other projects, Udacity reviewers will use this rubric to assess your project and provide feedback. If your project does not meet specifications, you can make changes and resubmit.


# Instructions

## Setup virtual environment

```
conda create -n dend_capstone python=3.7.3 anaconda
conda activate dend_capstone
pip install apache-airflow
pip install boto3
pip install psycopg2-binary
pip install pyspark==2.4.3
```

## Install Java

https://towardsdatascience.com/installing-pyspark-with-java-8-on-ubuntu-18-04-6a9dea915b5b


## Setup airflow 

- assistance from here: http://michal.karzynski.pl/blog/2017/03/19/developing-workflows-with-apache-airflow/

```
cd ~/Projects/udacity_data_engineering_nanodegree/5data_pipelines_w_airflow/project
export AIRFLOW_HOME=`pwd`
airflow version
airflow initdb
```

- Add connections to Airflow
  - Follow images available in the `./img` folder for AWS and Redshift credentials, respectively

- Add connection to AWS credentials using IAM role
  - Conn Id: aws_credentials
  - Conn Type: Amazon Web Services
  - Login: airflow_redshift_user IAM role, Access Key
  - Password: airflow_redshift_user IAM role, Secret Key
- Add connection to Redshift credentials
  - Conn Id: redshift
  - Conn Type: Postgres
  - Host: (endpoint copied from AWS Redshift)
  - Schema: dev 
  - Login: (redshift DB user)
  - Password: (redshift DB password)
  - Port: 5439

## Running DAG

- In two separate terminals run these shared steps:
```
cd ~/Projects/udacity_data_engineering_nanodegree/5data_pipelines_w_airflow/project
export AIRFLOW_HOME=`pwd`
conda activate udacity_p5
```
- And then, in Terminal 1:
```
airflow webserver
```

- In a new terminal window 
```
airflow scheduler
```

- Access at: http://localhost:8080/