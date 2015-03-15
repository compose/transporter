Source({"name": "rethink1", "namespace": "test.transporter", "tail": true, "debug": true}).
  transform({"filename": "transformers/rethink_id_to_elasticsearch_id.js"}).
  save({"name": "locales", "namespace": "test.transporter"})
