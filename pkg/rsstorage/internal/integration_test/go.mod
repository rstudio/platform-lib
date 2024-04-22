module github.com/rstudio/platform-lib/pkg/rsstorage/internal/integration_test

go 1.20

require (
	github.com/aws/aws-sdk-go v1.51.25
	github.com/fortytw2/leaktest v1.3.0
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v4 v4.18.3
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
)

// Require platform-lib dependencies. Note that the versions don't matter here
// since all get replaced by the `replace` directive below. The versions here
// should match some valid version to make the IDE happy, but they're ignored
// by Go when compiling.
require (
	github.com/rstudio/platform-lib/pkg/rsstorage v1.0.2
	github.com/rstudio/platform-lib/pkg/rsstorage/servers/file v1.0.2
	github.com/rstudio/platform-lib/pkg/rsstorage/servers/postgres v1.0.2
	github.com/rstudio/platform-lib/pkg/rsstorage/servers/s3server v1.0.2
)

// Always use the rsstorage code that is checked out.
replace (
	github.com/rstudio/platform-lib/pkg/rsstorage => ../../
	github.com/rstudio/platform-lib/pkg/rsstorage/servers/file => ../../servers/file
	github.com/rstudio/platform-lib/pkg/rsstorage/servers/postgres => ../../servers/postgres
	github.com/rstudio/platform-lib/pkg/rsstorage/servers/s3server => ../../servers/s3server
)

require (
	github.com/c2h5oh/datasize v0.0.0-20200825124411-48ed595a09d2 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.14.3 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.3 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/pgtype v1.14.0 // indirect
	github.com/jackc/puddle v1.3.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	golang.org/x/crypto v0.20.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)
