module github.com/rstudio/platform-lib/pkg/rsnotify/listeners/pq

go 1.17

require (
	github.com/fortytw2/leaktest v1.3.0
	github.com/jmoiron/sqlx v0.0.0-20170430194603-d9bd385d68c0
	github.com/lib/pq v1.10.2
	github.com/rstudio/platform-lib/pkg/rsnotify v0.1.8
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f
)

replace github.com/rstudio/platform-lib/pkg/rsnotify => ../../

require (
	github.com/go-sql-driver/mysql v1.6.0 // indirect
	github.com/kr/text v0.1.0 // indirect
	github.com/mattn/go-sqlite3 v1.6.1-0.20180419073257-a72efd674f65 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
)
