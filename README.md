# DAGs Airflow utilisés pour Trackdéchets

Ce dépôt contient les DAGs et plugins Airflow utilisés par [Trackdéchets](https://trackdechets.beta.gouv.fr/). Il remplace en partie l'ancien dépôt [MTES-MCT/trackdechets-etl](https://github.com/MTES-MCT/trackdechets-etl) qui contenait à la fois les DAGs et la logique de déploiement d'Airflow (anciennement hébergé sur Scalingo).

## Développement en local

Pour pouvoir développer en local, il suffit de cloner ce repository et de le monter comme un volume dans un container Airflow.
Airflow propose un fichier `docker-compose.yaml` et une procédure documenté pour faire tourner Airflow localement : [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/2.3.2/start/docker.html#docker-compose-yaml).
Il faut modifier les lignes suivantes du fichier `docker-compose.yaml` avant de lancer la commande `docker-compose up airflow-init` :
```
volumes:
- ./dags:/opt/airflow/dags
- ./logs:/opt/airflow/logs
- ./plugins:/opt/airflow/plugins
```
par :
```
volumes:
- /chemin/vers/repo/trackdechets-airflow-workspace:/opt/airflow/dags
- /chemin/vers/repo/trackdechets-airflow-workspace:/opt/airflow/logs
- /chemin/vers/repo/trackdechets-airflow-workspace:/opt/airflow/plugins
```
en remplaçant : `/chemin/vers/repo/trackdechets-airflow-workspace` par le chemin de ce repository sur votre ordinateur.

## Déploiement

Pour déployer en production vos DAGs ou plugins, il suffit de créer une pull request sur la branche main et une fois celle-ci merge, une github action synchronisera le repository avec l'instance airflow de production.
