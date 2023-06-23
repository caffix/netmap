// Copyright © by Jeff Foley 2017-2023. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.
// SPDX-License-Identifier: Apache-2.0

package netmap

import (
	"context"
	"net"
	"testing"
)

func TestNetblock(t *testing.T) {
	g := NewGraph(NewCayleyGraphMemory())
	for _, tt := range graphTest {
		t.Run("Testing UpsertNetblock...", func(t *testing.T) {
			got, err := g.UpsertNetblock(context.Background(), tt.CIDR, tt.Source, tt.EventID)
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
