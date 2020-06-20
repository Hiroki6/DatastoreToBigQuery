# Deploy
deploy:
	pip install -e .; \
	echo `pip list`; \
	python pipeline.py --runner DataflowRunner --project $(PROJECT) --staging_location gs://$(PROJECT)-dataflow/staging --temp_location gs://$(PROJECT)-dataflow/temp --template_location gs://$(PROJECT)-dataflow/templates/datastore-backup

# Run
run:
	gcloud dataflow jobs run datastore_backup --project $(PROJECT) --gcs-location gs://$(PROJECT)-dataflow/templates/datastore-backup --region asia-northeast1 --parameters dataset=$(TARGET_DATASET)
