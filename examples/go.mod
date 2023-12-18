module github.com/rstudio/platform-lib/examples

go 1.20

require (
	github.com/gomarkdown/markdown v0.0.0-20220310201231-552c6011c0b8
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/jackc/pgx/v4 v4.14.1
	github.com/jmoiron/sqlx v0.0.0-20170430194603-d9bd385d68c0
	github.com/lib/pq v1.10.2
	github.com/mattn/go-sqlite3 v1.14.9
	github.com/pkg/errors v0.9.1
	github.com/spf13/cobra v1.2.1
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
	gorm.io/driver/sqlite v1.3.1
	gorm.io/gorm v1.23.2
)

// Require platform-lib dependencies. Note that the versions don't matter here
// since all get replaced by the `replace` directive below. The versions here
// should match some valid version to make the IDE happy, but they're ignored
// by Go when compiling.
require (
	github.com/rstudio/platform-lib/pkg/rscache v0.0.0-00010101000000-000000000000
	github.com/rstudio/platform-lib/pkg/rslog v0.0.0-00010101000000-000000000000
	github.com/rstudio/platform-lib/pkg/rsnotify v1.5.2
	github.com/rstudio/platform-lib/pkg/rsnotify/listeners/local v1.4.1
	github.com/rstudio/platform-lib/pkg/rsnotify/listeners/postgrespgx v0.0.0-00010101000000-000000000000
	github.com/rstudio/platform-lib/pkg/rsnotify/listeners/postgrespq v0.0.0-00010101000000-000000000000
	github.com/rstudio/platform-lib/pkg/rsqueue v1.0.0
	github.com/rstudio/platform-lib/pkg/rsqueue/impls/database v0.0.0-00010101000000-000000000000
	github.com/rstudio/platform-lib/pkg/rsstorage v0.3.0
	github.com/rstudio/platform-lib/pkg/rsstorage/servers/file v0.2.0
)

// We want the example app to always use the latest platform-lib dependencies
// (from the main branch), so add replace directives for all of them.
replace (
	github.com/rstudio/platform-lib/pkg/rscache => ../pkg/rscache
	github.com/rstudio/platform-lib/pkg/rslog => ../pkg/rslog
	github.com/rstudio/platform-lib/pkg/rsnotify => ../pkg/rsnotify
	github.com/rstudio/platform-lib/pkg/rsnotify/listeners/local => ../pkg/rsnotify/listeners/local
	github.com/rstudio/platform-lib/pkg/rsnotify/listeners/postgrespgx => ../pkg/rsnotify/listeners/postgrespgx
	github.com/rstudio/platform-lib/pkg/rsnotify/listeners/postgrespq => ../pkg/rsnotify/listeners/postgrespq
	github.com/rstudio/platform-lib/pkg/rsqueue => ../pkg/rsqueue
	github.com/rstudio/platform-lib/pkg/rsqueue/impls/database => ../pkg/rsqueue/impls/database
	github.com/rstudio/platform-lib/pkg/rsstorage => ../pkg/rsstorage
	github.com/rstudio/platform-lib/pkg/rsstorage/servers/file => ../pkg/rsstorage/servers/file
)

require (
	github.com/c2h5oh/datasize v0.0.0-20200825124411-48ed595a09d2 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/dgraph-io/ristretto v0.1.0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.10.1 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.2.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	github.com/jackc/pgtype v1.9.1 // indirect
	github.com/jackc/puddle v1.2.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.4 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/magefile/mage v1.10.0 // indirect
	github.com/sirupsen/logrus v1.8.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	golang.org/x/crypto v0.17.0 // indirect
	golang.org/x/sys v0.15.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)
