module github.com/rstudio/platform-lib/pkg/rsstorage/servers/s3server

go 1.17

require (
	github.com/aws/aws-sdk-go v1.42.36
	github.com/fortytw2/leaktest v1.3.0
	github.com/google/uuid v1.3.0
	github.com/jarcoal/httpmock v1.1.0
	github.com/rstudio/platform-lib/pkg/rsstorage v1.0.2
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
)

replace github.com/rstudio/platform-lib/pkg/rsstorage => ../../

require (
	github.com/c2h5oh/datasize v0.0.0-20200825124411-48ed595a09d2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)