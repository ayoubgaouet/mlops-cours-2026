# Rapport comparatif — Kubeflow Pipelines vs Airflow (Lab 2)

**Binôme :** Ayoub Gaouet · **Date :** 23/11/2026 · **Machine :** VM VirtualBox (Vagrant),
4 vCPU, 12 Go de RAM, Ubuntu 24.04, sur un PC Windows 11 ·
**Dataset :** german-credit

## 1. Contexte

Le pipeline de scoring crédit compte six étapes : ingest, validate, prepare, train,
evaluate, register. Il tourne sur le cluster kind du cours (1 control-plane, 2 workers) et
utilise le dataset german-credit (1000 lignes, 30 % de « bad »). Les deux orchestrateurs
exécutent la même image `localhost:5001/scoring-pipeline:1.0.0`, qui contient le code du
Lab 1 (`src/`), `dataset.yaml` et `params.yaml`. Une étape correspond à un pod, avec le même
`python -m src.<étape>` que `dvc repro`. Ils partagent aussi le serveur MLflow du cluster :
les runs `launched_by=kfp` et `launched_by=airflow` obtiennent le même `roc_auc` (0,8094) et
promeuvent `scoring-model` avec les mêmes alias. Seul le transport des données diffère :
artefacts typés pour KFP, dossier de travail S3 et XCom pour Airflow.

## 2. Résultats quantitatifs

| Mesure | KFP | Airflow |
|---|---|---|
| Installation de zéro (min) | ~30 (manifests) + ~120 de dépannage (DNS des nœuds, MySQL) | ~25 (dont 21 de téléchargement d'images) |
| Temps jusqu'au premier run réussi (min) | ~66 (44 bloquées sur `ImagePullBackOff`) | ~15 (DAG à forcer avec `reserialize`) |
| Durée d'un run complet (min) | ~15 | ~11 |
| Relance avec les mêmes paramètres (min) | ~45 (data en cache, mais VM saturée) | ~11 (aucun cache) |
| Hot-reload : modifier un step et voir le résultat (min) | ~1 + run (recompiler, uploader) ; nouvelle image si `src/` | < 1 + run (DAG relu) ; nouvelle image si `src/` |
| Lignes de code propres à l'orchestrateur | 284 | 71 |
| Mémoire consommée par l'outil (Go, `docker stats`) | ~0,8 à 1 | ~1,8 |

## 3. Analyse qualitative

### Developer experience

KFP impose plus de code (284 lignes contre 71) : chaque composant déclare des entrées et
sorties typées (`Input[Dataset]`, `Output[Model]`). En échange, l'UI affiche les métadonnées
(md5 identique à `dvc.lock`, métriques, matrice de confusion) sans effort, et `kfp.local`
teste tout le DAG en une minute et demie, sans cluster. Le DAG Airflow est plus court et plus
lisible, mais tout transite par un préfixe S3 et par XCom, à gérer soi-même. Côté
installation, Airflow en Docker Compose a été bien plus simple que les manifests KFP
(MySQL, Argo, metadata-grpc…).

### Scaling

Les deux lancent un pod par étape dans Kubernetes et règlent les ressources par étape
(`set_memory_limit` pour l'un, `container_resources` pour l'autre). KFP apporte en plus le
cache, les retries et les timeouts déclaratifs, compilés dans le YAML. Sur une VM de 12 Go,
faire tourner les deux en même temps a saturé la machine : nœuds `NotReady`, API KFP en `504`.

### Debugging

KFP montre le lineage en un clic (run KFP → run MLflow → modèle), mais ses pannes
d'infrastructure sont opaques : un driver bloqué 8 h, un MySQL à moitié initialisé. Airflow
affiche clairement l'état et les logs de chaque tâche, et une tâche se rejoue en local en 3 s
(`pipelines.stage --workspace /tmp/ws`).

## 4. Recommandation

Je choisirais KFP pour une équipe ML sur un vrai cluster Kubernetes, qui itère beaucoup sur
les hyper-paramètres et a besoin de lineage, d'artefacts typés et de cache, à condition
d'avoir quelqu'un pour exploiter la plateforme. Je choisirais Airflow pour orchestrer un
pipeline stable, planifié (cron) et mêlé à d'autres traitements de données, ou quand
l'équipe connaît déjà Airflow. Le KubernetesPodOperator permet alors de garder la même image
que KFP.

## 5. Limites de la comparaison

Ce lab ne compare qu'un petit dataset sur une VM de 12 Go, saturée par moments : les durées
mesurées dépendent surtout de la charge de la machine, pas des outils. KFP n'a pas été testé
sur un vrai cluster (GKE, EKS), ni en mode multi-utilisateur. Airflow a tourné en Docker
Compose avec le CeleryExecutor, et non avec le KubernetesExecutor ou un déploiement Helm. Le
parallélisme (deux `train` en parallèle) et la planification n'ont pas été mesurés, et une
seule exécution de chaque n'a pas de valeur statistique.
