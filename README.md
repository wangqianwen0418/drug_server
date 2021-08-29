### run locally

```
conda env create -n [ENV_NAME] -f requirements.txt
conda activate [ENV_NAME]
python drug_server/application.py
```

### run on AWS Elastic Beanstalk

```
pip freeze > requirements.txt
```

entry point is application.py

initialize eb CLI repo

```
eb init -p python-3.6 drug-server --region us-east-2
```

create environment and deploy the application

```
eb create flask-env
```

update application

```
eb deploy
```


