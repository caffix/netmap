// Copyright © by Jeff Foley 2017-2023. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.
// SPDX-License-Identifier: Apache-2.0

package netmap

import (
	"context"
	"testing"

	"github.com/owasp-amass/open-asset-model/network"
)

func TestAddress(t *testing.T) {
	g := NewGraph("memory", "", "")
	defer g.Remove()

	t.Run("Testing UpsertAddress...", func(t *testing.T) {
		want := "192.168.1.1"

		if got, err := g.UpsertAddress(context.Background(), want); err != nil {
			t.Errorf("error inserting address:%v\n", err)
		} else if a, ok := got.Asset.(*network.IPAddress); !ok || a.Address.String() != want {
			t.Error("IP address was not returned properly")
		}
	})

	t.Run("Testing UpsertA...", func(t *testing.T) {
		err := g.UpsertA(context.Background(), "owasp.org", "192.168.1.1")
		if err != nil {
			t.Errorf("error inserting fqdn: %v", err)
		}
	})

	t.Run("Testing UpsertAAAA...", func(t *testing.T) {
		err := g.UpsertAAAA(context.Background(), "owasp.org", "2001:0db8:85a3:0000:0000:8a2e:0370:7334")

		if err != nil {
			t.Errorf("error inserting AAAA record: %v", err)
		}
	})
}

func TestNameToAddrs(t *testing.T) {
	fqdn := "caffix.net"
	addr := "192.168.1.1"

	g := NewGraph("memory", "", "")
	defer g.Remove()

	ctx := context.Background()
	if _, err := g.NamesToAddrs(ctx, fqdn); err == nil {
		t.Errorf("did not return an error when provided parameters not existing in the graph")
	}

	_ = g.UpsertA(ctx, fqdn, addr)
	if pairs, err := g.NamesToAddrs(ctx, fqdn); err != nil ||
		pairs[0].FQDN.Name != fqdn || pairs[0].Addr.Address.String() != addr {
		t.Errorf("failed to obtain the name / address pairs: %v", err)
	}

	if pairs, err := g.NamesToAddrs(ctx, "doesnot.exist"); err == nil {
		t.Errorf("did not return an error when provided a name not existing in the graph: %v", pairs)
	}
}
