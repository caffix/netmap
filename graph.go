// Copyright © by Jeff Foley 2017-2023. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.
// SPDX-License-Identifier: Apache-2.0

package netmap

import (
	"fmt"
	"math/rand"

	db "github.com/owasp-amass/asset-db"
	"github.com/owasp-amass/asset-db/repository"
)

// Graph is the object for managing a network infrastructure link graph.
type Graph struct {
	DB *db.AssetDB
}

// NewGraph returns an intialized Graph object.
func NewGraph(system, path string, options string) *Graph {
	var dsn string
	var dbtype repository.DBType

	switch system {
	case "memory":
		dbtype = repository.SQLite
		dsn = fmt.Sprintf("file:sqlite%d?mode=memory&cache=shared", rand.Int31n(100))
	case "local":
		dbtype = repository.SQLite
		dsn = path
	case "postgres":
		dbtype = repository.Postgres
		dsn = path
	default:
		return nil
	}

	store := db.New(dbtype, dsn)
	if store == nil {
		return nil
	}

	return &Graph{DB: store}
}
