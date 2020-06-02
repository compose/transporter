// Copyright 2014 The Transporter Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pipeline provides all adaptoremented functionality to move
// data through transporter.
//
// A transporter pipeline consists of a  tree of Nodes, with the root Node attached to the source database,
// and each child node is either a data transformer or a database sink.
// Node's can be defined like:
//
//   a, err := adaptor.GetAdaptor("mongodb", map[string]interface{}{"uri": "mongo://localhost:27017"})
//   if err != nil {
//     fmt.Println(err)
//     os.Exit(1)
//   }
//   source := pipeline.NewNodeWithOptions(
//     "source", "mongo", "/.*/",
//     pipeline.WithClient(a),
//     pipeline.WithReader(a),
//   )
//   f, err := adaptor.GetAdaptor("file", map[string]interface{}{"uri": "stdout://"})
//   sink := pipeline.NewNodeWithOptions(
//     "out", "file", "/.*/",
//     pipeline.WithClient(f),
//     pipeline.WithWriter(f),
//     pipeline.WithParent(source),
//   )
//
// and pipelines can be defined :
//   pipeline, err := transporter.NewPipeline(source, events.NewNoopEmitter(), 1*time.Second)
//   if err != nil {
//     fmt.Println(err)
//     os.Exit(1)
//   }
//   pipeline.Run()
//
// the event emitter's are defined in transporter/events, and are used to deliver error/metrics/etc about the running process
package pipeline
