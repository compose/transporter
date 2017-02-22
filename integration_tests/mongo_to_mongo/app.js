Source({name:"enron_source_mongo", namespace: 'enron.emails'})
  .save({name:"enron_sink_mongo", namespace: 'enron.emails'});
