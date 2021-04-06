// Copyright 2017-2021 Jeff Foley. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package netmap

import (
	"testing"

	"github.com/cayleygraph/quad"
)

func TestNewCayleyGraph(t *testing.T) {
	if g := NewCayleyGraph("", "fake_path", ""); g != nil {
		t.Errorf("NewCayleyGraph returned no error when provided an empty system argument")
	}

	if g := NewCayleyGraph("local", "", ""); g != nil {
		t.Errorf("NewCayleyGraph returned no error when provided an empty path argument")
	}
}

func TestDumpGraph(t *testing.T) {
	g := NewCayleyGraphMemory()

	if dump := g.DumpGraph(); dump != "" {
		t.Errorf("DumpGraph returned a non-empty string for an empty graph")
	}

	vBob := quad.IRI("Bob")
	vType := quad.IRI("type")
	// setup the initial data in the graph
	if err := g.store.AddQuad(quad.Make(vBob, vType, "Person", nil)); err != nil {
		t.Errorf("Failed to add the bob quad")
	}

	if dump := g.DumpGraph(); dump == "" {
		t.Errorf("DumpGraph returned an empty string for a non-empty graph")
	}
}
