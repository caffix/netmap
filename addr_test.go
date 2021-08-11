// Copyright 2017-2021 Jeff Foley. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package netmap

import (
	"context"
	"testing"
)

func TestAddress(t *testing.T) {
	g := NewGraph(NewCayleyGraphMemory())
	defer g.Close()

	for _, tt := range graphTest {
		t.Run("Testing UpsertAddress...", func(t *testing.T) {
			got, err := g.UpsertAddress(context.Background(), tt.Addr, tt.Source, tt.EventID)

			if err != nil {
				t.Errorf("Error inserting address:%v\n", err)
			}

			if got != tt.Addr {
				t.Errorf("Name of node was not returned properly.\nExpected:%v\nGot:%v\n", tt.Addr, got)
			}
		})

		t.Run("Testing UpsertA...", func(t *testing.T) {
			err := g.UpsertA(context.Background(), tt.FQDN, tt.Addr, tt.Source, tt.EventID)
			if err != nil {
				t.Errorf("Error inserting fqdn:%v\n", err)
			}
		})

		t.Run("Testing UpsertAAAA...", func(t *testing.T) {
			err := g.UpsertAAAA(context.Background(), tt.FQDN, tt.Addr, tt.Source, tt.EventID)

			if err != nil {
				t.Errorf("Error inserting AAAA record: %v\n", err)
			}
		})
	}
}

func TestNameToAddrs(t *testing.T) {
	fqdn := "caffix.net"
	addr := "192.168.1.1"
	event := "uniqueID"

	g := NewGraph(NewCayleyGraphMemory())
	defer g.Close()

	ctx := context.Background()
	if _, err := g.NamesToAddrs(ctx, event, fqdn); err == nil {
		t.Errorf("Did not return an error when provided parameters not existing in the graph")
	}

	g.UpsertA(ctx, fqdn, addr, "test", event)
	if pairs, err := g.NamesToAddrs(ctx, event); err != nil ||
		pairs[0].Name != fqdn || pairs[0].Addr != addr {
		t.Errorf("Failed to obtain the name / address pairs: %v", err)
	}

	if pairs, err := g.NamesToAddrs(ctx, event, fqdn); err != nil ||
		pairs[0].Name != fqdn || pairs[0].Addr != addr {
		t.Errorf("Failed to obtain the name / address pairs: %v", err)
	}

	if pairs, err := g.NamesToAddrs(ctx, event, "doesnot.exist"); err == nil {
		t.Errorf("Did not return an error when provided a name not existing in the graph: %v", pairs)
	}
}
