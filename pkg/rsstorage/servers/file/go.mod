module github.com/rstudio/platform-lib/pkg/rsstorage/servers/file

go 1.17

require (
	github.com/c2h5oh/datasize v0.0.0-20200825124411-48ed595a09d2
	github.com/minio/minio v0.0.0-20210323145707-da70e6ddf63c
	github.com/rstudio/platform-lib/pkg/rsstorage v0.1.0
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
)

replace github.com/rstudio/platform-lib/pkg/rsstorage => ../../

require (
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/montanaflynn/stats v0.5.0 // indirect
	github.com/ncw/directio v1.0.5 // indirect
	golang.org/x/sys v0.0.0-20210615035016-665e8c7367d1 // indirect
)
