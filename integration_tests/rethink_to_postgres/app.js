Source({name:"enron_source_rethink", namespace: 'enron.emails'})
  .save({name:"enron_sink_postgres", namespace: 'enron.emails'});
