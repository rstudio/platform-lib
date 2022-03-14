module github.com/rstudio/platform-lib/pkg/rselection/impls/local

go 1.17

require (
	github.com/fortytw2/leaktest v1.3.0
	github.com/rstudio/platform-lib/pkg/rselection v1.0.0
	github.com/rstudio/platform-lib/pkg/rsnotify v1.2.0
	github.com/rstudio/platform-lib/pkg/rsnotify/listeners/local v1.2.0
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
)

replace github.com/rstudio/platform-lib/pkg/rselection => ../../

require (
	github.com/google/uuid v1.1.2 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/kr/text v0.2.0 // indirect
)
