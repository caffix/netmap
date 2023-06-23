// Copyright © by Jeff Foley 2017-2023. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.
// SPDX-License-Identifier: Apache-2.0

package netmap

import (
	"context"
	"strconv"
	"testing"
)

func TestAS(t *testing.T) {
	g := NewGraph(NewCayleyGraphMemory())
	defer g.Close()

	newdesc := "Great ASN"
	for _, tt := range graphTest {
		t.Run("Testing UpsertAS...", func(t *testing.T) {
			got, err := g.UpsertAS(context.Background(), tt.ASNString, tt.Desc, tt.Source, tt.EventID)

			if err != nil {
				t.Errorf("Error inserting AS: %v\n", err)
			}
			if got != tt.ASNString {
				t.Errorf("Returned value for InsertAS is not the same as test asn string:\ngot: %v\nwant: %v\n", got, tt.ASNString)
			}
		})

		t.Run("Testing UpsertInfrastructure", func(t *testing.T) {
			err := g.UpsertInfrastructure(context.Background(), tt.ASN, newdesc, tt.Addr, tt.CIDR, tt.Source, tt.EventID)
			if err != nil {
				t.Errorf("Error inserting infrastructure: %v\n", err)
			}
		})

		t.Run("Testing ReadASDescription", func(t *testing.T) {
			var got string

			if asn, err := strconv.Atoi(tt.ASNString); err == nil {
				got = g.ReadASDescription(context.Background(), asn)
			}

			if got != newdesc {
				t.Errorf("Expected: %v\nGot: %v\n", newdesc, got)
			}
		})

		t.Run("Testing ReadASPrefixes", func(t *testing.T) {
			var got []string

			if asn, err := strconv.Atoi(tt.ASNString); err == nil {
				got = g.ReadASPrefixes(context.Background(), asn)
			}

			if len(got) != 1 || got[0] != tt.CIDR {
				t.Errorf("Expected: %v\nGot: %v\n", tt.CIDR, got)
			}
		})
	}
}
