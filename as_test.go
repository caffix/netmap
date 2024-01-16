// Copyright © by Jeff Foley 2017-2023. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.
// SPDX-License-Identifier: Apache-2.0

package netmap

import (
	"context"
	"testing"
	"time"

	"github.com/owasp-amass/open-asset-model/network"
)

func TestAS(t *testing.T) {
	g := NewGraph("memory", "", "")
	defer g.Remove()

	asn := 667
	newdesc := "Great AS"
	cidr := "10.0.0.0/8"
	addr := "10.0.0.1"

	t.Run("Testing UpsertAS...", func(t *testing.T) {
		got, err := g.UpsertAS(context.Background(), asn, newdesc)
		if err != nil {
			t.Errorf("error inserting AS: %v\n", err)
		}

		if as, ok := got.Asset.(*network.AutonomousSystem); !ok {
			t.Error("failed to read the inserted autonomous system")
		} else if as.Number != asn {
			t.Errorf("returned value for InsertAS is not the same as the test asn value. got: %d, want: %d", as.Number, asn)
		}
	})

	t.Run("Testing UpsertInfrastructure", func(t *testing.T) {
		err := g.UpsertInfrastructure(context.Background(), asn, newdesc, addr, cidr)
		if err != nil {
			t.Errorf("error inserting infrastructure: %v", err)
		}
	})

	t.Run("Testing ReadASDescription", func(t *testing.T) {
		got := g.ReadASDescription(context.Background(), asn, time.Time{})

		if got != newdesc {
			t.Errorf("expected: %v, got: %v", newdesc, got)
		}
	})

	t.Run("Testing ReadASPrefixes", func(t *testing.T) {
		got := g.ReadASPrefixes(context.Background(), asn, time.Time{})

		if len(got) != 1 || got[0] != cidr {
			t.Errorf("expected: %v, got: %v\n", cidr, got)
		}
	})
}
