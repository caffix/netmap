// Copyright © by Jeff Foley 2017-2023. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.
// SPDX-License-Identifier: Apache-2.0

package netmap

import (
	"embed"
	"fmt"
	"math/rand"

	db "github.com/owasp-amass/asset-db"
	"github.com/owasp-amass/asset-db/migrations/postgres"
	"github.com/owasp-amass/asset-db/migrations/sqlite3"
	"github.com/owasp-amass/asset-db/repository"
	migrate "github.com/rubenv/sql-migrate"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// Graph is the object for managing a network infrastructure link graph.
type Graph struct {
	DB  *db.AssetDB
	dsn string
}

// NewGraph returns an intialized Graph object.
func NewGraph(system, path string, options string) *Graph {
	g := OpenGraph(system, path, options)
	if g == nil {
		return nil
	}

	var name string
	var fs embed.FS
	var database gorm.Dialector
	switch system {
	case "memory":
		fallthrough
	case "local":
		name = "sqlite3"
		fs = sqlite3.Migrations()
		database = sqlite.Open(g.dsn)
	case "postgres":
		name = "postgres"
		fs = postgres.Migrations()
		database = postgres.Open(g.dsn)
	}

	sql, err := gorm.Open(database, &gorm.Config{})
	if err != nil {
		return nil, err
	}

	migrationsSource := migrate.EmbedFileSystemMigrationSource{
		FileSystem: fs,
		Root:       "/",
	}

	_, err = migrate.Exec(sql.DB(), name, migrationsSource, migrate.Up)
	if err != nil {
		panic(err)
	}
	return g
}

// OpenGraph opens and returns a netmap Graph object for the specified database.
func OpenGraph(system, path string, options string) *Graph {
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

	return &Graph{
		DB:  store,
		dsn: dsn,
	}
}
