module github.com/rstudio/platform-lib/pkg/rscache/internal/integration_test

go 1.20

require (
	github.com/dgraph-io/ristretto v0.1.1
	github.com/fortytw2/leaktest v1.3.0
	github.com/rstudio/platform-lib/pkg/rsstorage v0.3.0
	github.com/rstudio/platform-lib/pkg/rsstorage/servers/file v0.3.1
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
)

// Requires rscache, but version doesn't matter since it is always
// replaced (below) with what is checked out.
require github.com/rstudio/platform-lib/pkg/rscache v0.1.1

// Always use the rscache code that is checked out.
replace github.com/rstudio/platform-lib/pkg/rscache => ../../

require (
	github.com/c2h5oh/datasize v0.0.0-20200825124411-48ed595a09d2 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/minio/minio v0.0.0-20210323145707-da70e6ddf63c // indirect
	github.com/montanaflynn/stats v0.5.0 // indirect
	github.com/ncw/directio v1.0.5 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/stretchr/testify v1.7.0 // indirect
	golang.org/x/sys v0.1.0 // indirect
)
