// Copyright 2017-2021 Jeff Foley. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.

package netmap

import (
	"net"
	"testing"
)

func TestNetblock(t *testing.T) {
	g := NewGraph(NewCayleyGraphMemory())
	for _, tt := range graphTest {
		t.Run("Testing UpsertNetblock...", func(t *testing.T) {
			got, err := g.UpsertNetblock(tt.CIDR, tt.Source, tt.EventID)
			if err != nil {
				t.Errorf("Error inserting netblock.\n%v\n", err)

			}

			get, _, err := net.ParseCIDR(got.(string))
			want, _, _ := net.ParseCIDR(tt.CIDR)

			if err != nil {
				t.Errorf("Error parsing node's cidr info from netblock.\n%v\n", got)
			}
			if !net.IP.Equal(get, want) {
				t.Errorf("Expected: %v\nGot: %v\n", want, get)
			}
		})

	}

}
