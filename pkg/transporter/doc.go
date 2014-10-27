// Copyright 2014 The Transporter Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package transporter provides all adaptoremented functionality to move
// data through transporter.
//
// A transporter pipeline consists of a  tree of Nodes, with the root Node attached to the source database,
// and each child node is either a data transformer or a database sink.
// Node's can be defined like:
//
//   transporter.NewNode("source", "mongo", map[string]interface{}{"uri": "mongodb://localhost/, "namespace": "test.colln", "debug": false, "tail": true}).
//     Add(transporter.NewNode("out", "file", map[string]interface{}{"uri": "stdout://"}))
//
// and pipelines can be defined :
//   pipeline, err := transporter.NewPipeline(source, events.NewNoopEmitter(), 1*time.Second)
//   if err != nil {
//     fmt.Println(err)
//     os.Exit(1)
//   }
//   pipeline.Run()
//
// the event emitter's are defined in transporter/pkg/events, and are used to deliver error/metrics/etc about the running process

package transporter
