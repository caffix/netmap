// Copyright 2017-2021 Jeff Foley. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package netmap

import (
	"testing"
)

func TestUpsertSource(t *testing.T) {
	g := NewGraph(NewCayleyGraphMemory())
	defer g.Close()

	src := "FakeSource"
	got, err := g.UpsertSource(src)
	if err != nil {
		t.Errorf("Failed to insert source: %v", err)

	}
	if got != src {
		t.Errorf("Expected: %v, Got: %v", src, got)
	}
}

func TestNodeSources(t *testing.T) {
	g := NewGraph(NewCayleyGraphMemory())
	defer g.Close()

	event := "eventID"
	if _, err := g.NodeSources(Node(""), event); err == nil {
		t.Errorf("Failed to return an error when provided an invalid node")
	}

	id := "MyNode"
	if _, err := g.NodeSources(Node(id), event); err == nil {
		t.Errorf("Failed to return an error when provided an non-existent event")
	}

	_, _ = g.UpsertEvent(event)
	n, _ := g.UpsertNode(id, TypeFQDN)
	if _, err := g.NodeSources(n, event); err == nil {
		t.Errorf("Failed to return an error when provided a node with no in-edges")
	}

	srcs := []string{"src1", "src2", "src3", "src4", "src5", "src6", "src7", "src8", "src9", "src10"}
	// Enter the source references
	for _, src := range srcs {
		_ = g.AddNodeToEvent(n, src, event)
	}

	got, err := g.NodeSources(n, event)
	if err != nil {
		t.Errorf("Failed to extract sources for the node provided: %v", err)
	}

	if !checkTestResult(srcs, got) {
		t.Errorf("Expected:% v, Got: %v", srcs, got)
	}
}

func TestCacheSourceData(t *testing.T) {
	g := NewGraph(NewCayleyGraphMemory())
	defer g.Close()

	src := "fakesource"
	query := "domain.com"
	resp1 := "subssubssubs"
	if err := g.CacheSourceData(src, query, resp1); err != nil {
		t.Errorf("Failed to save the data source response")
	}
	if r, err := g.GetSourceData(src, query, 1); err != nil || r != resp1 {
		t.Errorf("Failed to extract the data source response from the graph")
	}

	resp2 := "thenewresponse"
	if err := g.CacheSourceData(src, query, resp2); err != nil {
		t.Errorf("Failed to save the updated data source response")
	}
	if r, err := g.GetSourceData(src, query, 1); err != nil || r != resp2 {
		t.Errorf("Failed to extract the data source response from the graph")
	}
}
