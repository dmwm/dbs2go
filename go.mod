module github.com/dmwm/dbs2go

go 1.18

require (
	github.com/dmwm/cmsauth v0.0.0-20220120183156-5495692d4ca7
	github.com/go-playground/validator/v10 v10.10.1
	github.com/google/uuid v1.3.0
	github.com/gorilla/csrf v1.7.1
	github.com/gorilla/mux v1.8.0
	github.com/graph-gophers/graphql-go v1.3.0
	github.com/lestrrat-go/file-rotatelogs v2.4.0+incompatible
	github.com/mattn/go-oci8 v0.1.1
	github.com/mattn/go-sqlite3 v1.14.12
	github.com/prometheus/procfs v0.7.3
	github.com/r3labs/diff/v2 v2.15.1
	github.com/shirou/gopsutil v3.21.11+incompatible
	github.com/ulule/limiter/v3 v3.10.0
	github.com/vkuznet/auth-proxy-server/logging v0.0.0-20220316171336-a7168957a1a4
	github.com/vkuznet/limiter v2.2.2+incompatible
	github.com/vkuznet/x509proxy v0.0.0-20210801171832-e47b94db99b6
	gopkg.in/rana/ora.v4 v4.1.15
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-playground/locales v0.14.0 // indirect
	github.com/go-playground/universal-translator v0.18.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/gorilla/securecookie v1.1.1 // indirect
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/lestrrat-go/strftime v1.0.5 // indirect
	github.com/opentracing/opentracing-go v1.1.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/tklauser/go-sysconf v0.3.10 // indirect
	github.com/tklauser/numcpus v0.4.0 // indirect
	github.com/vmihailenco/msgpack v4.0.4+incompatible // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	golang.org/x/crypto v0.0.0-20220214200702-86341886e292 // indirect
	golang.org/x/net v0.0.0-20211112202133-69e39bad7dc2 // indirect
	golang.org/x/sync v0.0.0-20201207232520-09787c993a3a // indirect
	golang.org/x/sys v0.0.0-20220227234510-4e6760a101f9 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/appengine v1.6.6 // indirect
	google.golang.org/protobuf v1.26.0 // indirect
)

replace github.com/ulule/limiter/v3 => github.com/vkuznet/limiter/v3 v3.10.2
