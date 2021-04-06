// Copyright 2017-2021 Jeff Foley. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package netmap

import (
	"strconv"
	"testing"
)

func TestAS(t *testing.T) {
	g := NewGraph(NewCayleyGraphMemory())
	defer g.Close()

	newdesc := "Great ASN"
	for _, tt := range graphTest {
		t.Run("Testing UpsertAS...", func(t *testing.T) {
			got, err := g.UpsertAS(tt.ASNString, tt.Desc, tt.Source, tt.EventID)

			if err != nil {
				t.Errorf("Error inserting AS: %v\n", err)
			}
			if got != tt.ASNString {
				t.Errorf("Returned value for InsertAS is not the same as test asn string:\ngot: %v\nwant: %v\n", got, tt.ASNString)
			}
		})

		t.Run("Testing UpsertInfrastructure", func(t *testing.T) {
			err := g.UpsertInfrastructure(tt.ASN, newdesc, tt.Addr, tt.CIDR, tt.Source, tt.EventID)
			if err != nil {
				t.Errorf("Error inserting infrastructure: %v\n", err)
			}
		})

		t.Run("Testing ReadASDescription", func(t *testing.T) {
			var got string

			if asn, err := strconv.Atoi(tt.ASNString); err == nil {
				got = g.ReadASDescription(asn)
			}

			if got != newdesc {
				t.Errorf("Expected: %v\nGot: %v\n", newdesc, got)
			}
		})
	}
}
