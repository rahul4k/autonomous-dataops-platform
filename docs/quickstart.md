# Quickstart — Staging on GCP (high-level)

## 1. Create a project

```bash
gcloud projects create adp-staging-$(whoami)-001 --name="adp-staging"
gcloud config set project adp-staging-$(whoami)-001
```

## 2. Enable required APIs

```bash
gcloud services enable composer.googleapis.com pubsub.googleapis.com storage.googleapis.com run.googleapis.com \
    dataproc.googleapis.com iam.googleapis.com secretmanager.googleapis.com
```

## 3. Create service account(s)

```bash
gcloud iam service-accounts create adp-agent-sa --display-name="ADP Agent Service Account"

gcloud projects add-iam-policy-binding $(gcloud config get-value project) \
  --member="serviceAccount:adp-agent-sa@$(gcloud config get-value project).iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"

# Add other roles as needed: composer.worker, pubsub.publisher/subscriber, run.invoker, dataproc.worker
```

## 4. Create a GCS bucket for logs/artifacts

```bash
BUCKET=adp-artifacts-$(gcloud config get-value project)
gsutil mb -l us-central1 gs://$BUCKET
```

## 5. Cloud Composer (optional / for production demo)

Create Composer environment in GCP console or:

```bash
gcloud composer environments create adp-composer \
  --location=us-central1 --zone=us-central1-a \
  --python-version=3 --node-count=3
```

Upload DAG to Composer's DAGs folder (via GCS path the Composer environment uses).

## 6. Dataproc for Spark jobs (optional)

```bash
gcloud dataproc clusters create adp-cluster --region=us-central1 --zone=us-central1-a \
  --single-node --image-version=2.2-debian10
```

## 7. Pub/Sub Topic for failure events

```bash
gcloud pubsub topics create adp-failure-events
```

## 8. Deploy Agent Orchestrator (Cloud Run)

Build Docker image and push to Artifact Registry or Container Registry.

Deploy Cloud Run service with SA `adp-agent-sa`.

---

## Notes & Security

- Use **least privilege** for service accounts.
- Use **Secret Manager** to store credentials. Do NOT embed keys in code.
- Restrict Pub/Sub, Cloud Run, and Dataproc IAM bindings to specific service accounts only.

---

## How to Run Locally (quick)

Create a Python venv and install basic deps. For running scripts we don't strictly need `pyspark` — these scripts are pure Python (simulated).

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\\Scripts\\activate
pip install --upgrade pip
pip install apache-airflow==2.6.3  # optional if using Airflow
# No need to install pyspark for these simulation scripts
```

Then run individual agent scripts:

```bash
python agents/log_reader/main.py --log-file sample.log
python agents/root_cause/main.py --rca-input rca_input.json
python agents/fixer_optimizer/main.py --patch-config patch_config.json
```

---

## Next Steps

1. Review `docs/architecture.md` for system design and data flow.
2. Check `docs/agent_prompts.md` for agent system prompts and instructions.
3. Deploy sample DAG to Composer and trigger via Pub/Sub event.
4. Monitor agent execution logs in Cloud Logging.
5. Review generated PRs in GitHub for validation.
