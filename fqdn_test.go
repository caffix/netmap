// Copyright 2017-2021 Jeff Foley. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package netmap

import (
	"context"
	"testing"
)

func TestFQDN(t *testing.T) {
	g := NewGraph(NewCayleyGraphMemory())
	defer g.Close()

	ctx := context.Background()
	for _, tt := range graphTest {
		t.Run("Testing UpsertFQDN...", func(t *testing.T) {

			got, err := g.UpsertFQDN(ctx, tt.FQDN, tt.Source, tt.EventID)

			if err != nil {
				t.Errorf("Failed inserting FQDN:\n%v", err)
			}

			if got != tt.FQDN {
				t.Errorf("Error expecting FQDN.\nGot:%v\nWant:%v\n", got, tt.FQDN)
			}

		})

		t.Run("Testing UpsertCNAME...", func(t *testing.T) {
			err := g.UpsertCNAME(ctx, tt.FQDN, tt.FQDN, tt.Source, tt.EventID)

			if err != nil {
				t.Errorf("Failed inserting CNAME.\n%v", err)
			}
		})

		t.Run("Testing IsCNAMENode...", func(t *testing.T) {
			got := g.IsCNAMENode(ctx, tt.FQDN)

			if got != true {
				t.Errorf("Failed to obtain CNAME from node: %v\n", got)
			}
		})

		t.Run("Testing UpsertPTR...", func(t *testing.T) {
			got := g.UpsertPTR(ctx, tt.FQDN, tt.FQDN, tt.Source, tt.EventID)
			if got != nil {
				t.Errorf("Failed to InsertPTR. \n%v\n", got)
			}
		})

		t.Run("Testing IsPTRNode...", func(t *testing.T) {
			got := g.IsPTRNode(ctx, tt.FQDN)
			if got != true {
				t.Errorf("Failed to find PTRNode.\n%v:%v\n", tt.FQDN, got)
			}
		})

		t.Run("Testing UpsertSRV...", func(t *testing.T) {
			got := g.UpsertSRV(ctx, tt.FQDN, tt.Service, tt.FQDN, tt.Source, tt.EventID)
			if got != nil {
				t.Errorf("Failed inserting service into database.\n%v\n", got)
			}
		})

		t.Run("Testing UpsertNS...", func(t *testing.T) {
			got := g.UpsertNS(ctx, tt.FQDN, tt.FQDN, tt.Source, tt.EventID)

			if got != nil {
				t.Errorf("Failed inserting NS record.\n%v\n", got)
			}
		})

		t.Run("Testing IsNSNode...", func(t *testing.T) {
			got := g.IsNSNode(ctx, tt.FQDN)
			if got == false {
				t.Errorf("Failed to locate NS node.\n%v\n", got)
			}
		})

		t.Run("Testing UpsertMX...", func(t *testing.T) {
			got := g.UpsertMX(ctx, tt.FQDN, tt.FQDN, tt.Source, tt.EventID)
			if got != nil {
				t.Errorf("Failure to insert MX record.\n%v\n", got)
			}
		})

		t.Run("Testing IsMXNode...", func(t *testing.T) {
			got := g.IsMXNode(ctx, tt.FQDN)
			if got != true {
				t.Errorf("Failed to locate MX node.")
			}
		})

		t.Run("Testing IsRootDomainNode...", func(t *testing.T) {
			got := g.IsRootDomainNode(ctx, "owasp.org")
			if got != true {
				t.Errorf("Failed to locate root domain node.")
			}
		})

		t.Run("Testing IsTLDNode...", func(t *testing.T) {
			got := g.IsTLDNode(ctx, "org")
			if got != true {
				t.Errorf("Failed to locate TLD node.")
			}
		})
	}
}
