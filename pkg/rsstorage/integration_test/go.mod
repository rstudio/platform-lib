module github.com/rstudio/platform-lib/pkg/rsstorage/integration_test

go 1.17

require (
	github.com/aws/aws-sdk-go v1.42.36
	github.com/fortytw2/leaktest v1.3.0
	github.com/google/uuid v1.3.0
	github.com/jackc/pgx/v4 v4.14.1
	github.com/rstudio/platform-lib/pkg/rsstorage v1.0.2
	github.com/rstudio/platform-lib/pkg/rsstorage/servers/file v1.0.2
	github.com/rstudio/platform-lib/pkg/rsstorage/servers/postgres v1.0.2
	github.com/rstudio/platform-lib/pkg/rsstorage/servers/s3server v1.0.2
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
)

replace (
	github.com/rstudio/platform-lib/pkg/rsstorage => ../
	github.com/rstudio/platform-lib/pkg/rsstorage/servers/file => ../servers/file
	github.com/rstudio/platform-lib/pkg/rsstorage/servers/postgres => ../servers/postgres
	github.com/rstudio/platform-lib/pkg/rsstorage/servers/s3server => ../servers/s3server
)

require (
	github.com/c2h5oh/datasize v0.0.0-20200825124411-48ed595a09d2 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.10.1 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.2.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	github.com/jackc/pgtype v1.9.1 // indirect
	github.com/jackc/puddle v1.2.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/minio/minio v0.0.0-20210323145707-da70e6ddf63c // indirect
	github.com/montanaflynn/stats v0.5.0 // indirect
	github.com/ncw/directio v1.0.5 // indirect
	golang.org/x/crypto v0.0.0-20210711020723-a769d52b0f97 // indirect
	golang.org/x/sys v0.0.0-20210615035016-665e8c7367d1 // indirect
	golang.org/x/text v0.3.6 // indirect
)
