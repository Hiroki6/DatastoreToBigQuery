# Deploy
deploy:
	pip install -e .; \
	echo `pip list`; \
	python pipeline.py --runner DataflowRunner --project $(PROJECT) --staging_location gs://$(PROJECT)-dataflow/staging --temp_location gs://$(PROJECT)-dataflow/temp --template_location gs://$(PROJECT)-dataflow/templates/ds2bq

DATE :=`env TZ=Asia/Tokyo date "+%Y%m%d-%H%M%S"`

# Run
run:
	gcloud dataflow jobs run datastore_backup_$(DATE) --project $(PROJECT) --gcs-location gs://$(PROJECT)-dataflow/templates/ds2bq --region asia-northeast1 --parameters dataset=$(TARGET_DATASET)
