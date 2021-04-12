// Copyright 2017-2021 Jeff Foley. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package netmap

import (
	"testing"
	"time"

	"github.com/caffix/stringset"
)

func checkTestResult(want, got []string) bool {
	wset := stringset.New(want...)
	gset := stringset.New(got...)
	if gset.Len() != wset.Len() {
		return false
	}

	wset.Subtract(gset)
	return wset.Len() == 0
}

func TestEvent(t *testing.T) {
	g := NewGraph(NewCayleyGraphMemory())
	defer g.Close()

	for _, tt := range graphTest {
		t.Run("Testing InsertEvent...", func(t *testing.T) {
			got, err := g.UpsertEvent(tt.EventID)
			if err != nil {
				t.Errorf("Error inserting event:%v\n", err)
			}
			if got != tt.EventID {
				t.Errorf("Inserting new event failed.\n Got:%v\nWant:%v\n", got, tt.EventID)
			}
		})

		nodeOne, err := g.UpsertFQDN(tt.FQDN, tt.Source, tt.EventID)
		if err != nil {
			t.Fatal("Error inserting node\n")
		}

		t.Run("Testing AddNodeToEvent...", func(t *testing.T) {
			err := g.AddNodeToEvent(nodeOne, tt.Source, tt.EventID)
			if err != nil {
				t.Errorf("Error adding node to event:%v\n", err)
			}
		})

		t.Run("Testing EventList...", func(t *testing.T) {
			if got := g.EventList(); len(got) < 1 || got[0] != tt.EventID {
				t.Errorf("EventList expected %v\nGot:%v\n", tt.EventID, got)
			}
		})

		t.Run("Testing InEventScope...", func(t *testing.T) {
			if !g.InEventScope(Node(tt.FQDN), tt.EventID) {
				t.Errorf("Failed to identify a node as in scope of the provided event")
			}
		})

		t.Run("Testing EventsInScope...", func(t *testing.T) {
			events := g.EventsInScope(tt.Domain)

			if len(events) == 0 || events[0] != tt.EventID {
				t.Errorf("Failed to return the event associated with the provided domain")
			}
		})

		t.Run("Testing EventFQDNs...", func(t *testing.T) {
			var found bool

			for _, fqdn := range g.EventFQDNs(tt.EventID) {
				if fqdn != tt.FQDN {
					found = true
				}
			}

			if !found {
				t.Errorf("Failed to return the FQDNs associated with the provided event")
			}
		})

		t.Run("Testing EventDomains...", func(t *testing.T) {
			var want []string
			got := g.EventDomains(tt.EventID)
			want = append(want, tt.Domain)

			if !checkTestResult(want, got) {
				t.Errorf("Error testing event domains.\nWant:%v\nGot:%v\n", want, got)
			}
		})

		t.Run("Testing EventSubdomains...", func(t *testing.T) {
			var want []string
			got := g.EventSubdomains(tt.EventID)
			want = append(want, tt.FQDN)

			if !checkTestResult(want, got) {
				t.Errorf("Error testing event subdomains.\nWant:%v\nGot:%v\n", want, got)
			}
		})

		t.Run("Testing EventDateRange...", func(t *testing.T) {
			time.Sleep(250 * time.Millisecond)
			now := time.Now()
			start, finish := g.EventDateRange(tt.EventID)

			if err != nil {
				t.Errorf("Error getting current time.\n%v\n", err)
			}

			if finish.After(now) {
				t.Errorf("Finish time is after current time.\nFinish:%v\nNow:%v\n", finish, now)
			}

			if now.Before(start) {
				t.Errorf("Current time is before start time.\nStart:%v\nNow:%v\n", start, now)
			}

		})
	}
}
