module github.com/rstudio/platform-lib/examples

go 1.17

require (
	github.com/jackc/pgx/v4 v4.14.1
	github.com/jmoiron/sqlx v0.0.0-20170430194603-d9bd385d68c0
	github.com/lib/pq v1.10.2
	github.com/rstudio/platform-lib/pkg/rslog v1.0.0
	github.com/rstudio/platform-lib/pkg/rsnotify v1.2.0
	github.com/rstudio/platform-lib/pkg/rsnotify/listeners/local v1.0.2
	github.com/rstudio/platform-lib/pkg/rsnotify/listeners/postgrespgx v1.2.0
	github.com/rstudio/platform-lib/pkg/rsnotify/listeners/postgrespq v0.0.1
	github.com/spf13/cobra v1.2.1
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f
)

replace (
	github.com/rstudio/platform-lib/pkg/rslog => ../pkg/rslog
	github.com/rstudio/platform-lib/pkg/rsnotify => ../pkg/rsnotify
	github.com/rstudio/platform-lib/pkg/rsnotify/listeners/local => ../pkg/rsnotify/listeners/local
	github.com/rstudio/platform-lib/pkg/rsnotify/listeners/postgrespgx => ../pkg/rsnotify/listeners/postgrespgx
	github.com/rstudio/platform-lib/pkg/rsnotify/listeners/postgrespq => ../pkg/rsnotify/listeners/postgrespq
)

require (
	github.com/google/uuid v1.1.2 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.10.1 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.2.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	github.com/jackc/pgtype v1.9.1 // indirect
	github.com/jackc/puddle v1.2.0 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.3 // indirect
	github.com/kr/text v0.1.0 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/sirupsen/logrus v1.6.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	golang.org/x/crypto v0.0.0-20210711020723-a769d52b0f97 // indirect
	golang.org/x/sys v0.0.0-20210615035016-665e8c7367d1 // indirect
	golang.org/x/text v0.3.6 // indirect
)
