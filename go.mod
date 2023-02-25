module github.com/dmwm/dbs2go

go 1.20

require (
	github.com/dmwm/cmsauth v0.0.0-20230224144745-c57dbeca74a3
	github.com/go-playground/validator/v10 v10.11.2
	github.com/google/uuid v1.3.0
	github.com/gorilla/csrf v1.7.1
	github.com/gorilla/mux v1.8.0
	github.com/graph-gophers/graphql-go v1.5.0
	github.com/lestrrat-go/file-rotatelogs v2.4.0+incompatible
	github.com/mattn/go-oci8 v0.1.1
	github.com/mattn/go-sqlite3 v1.14.16
	github.com/prometheus/procfs v0.9.0
	github.com/r3labs/diff/v3 v3.0.0
	github.com/shirou/gopsutil v3.21.11+incompatible
	github.com/ulule/limiter/v3 v3.11.0
	github.com/vkuznet/auth-proxy-server/logging v0.0.0-20230224155500-18f9e3f9c368
	github.com/vkuznet/x509proxy v0.0.0-20210801171832-e47b94db99b6
	golang.org/x/exp v0.0.0-20230224173230-c95f2b4c22f2
	golang.org/x/exp/errors v0.0.0-20230224173230-c95f2b4c22f2
	gopkg.in/rana/ora.v4 v4.1.15
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/gorilla/securecookie v1.1.1 // indirect
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/lestrrat-go/strftime v1.0.6 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/tklauser/go-sysconf v0.3.11 // indirect
	github.com/tklauser/numcpus v0.6.0 // indirect
	github.com/vmihailenco/msgpack v4.0.4+incompatible // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	golang.org/x/crypto v0.6.0 // indirect
	golang.org/x/net v0.6.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	google.golang.org/appengine v1.6.6 // indirect
	google.golang.org/protobuf v1.26.0 // indirect
)

replace github.com/ulule/limiter/v3 => github.com/vkuznet/limiter/v3 v3.10.2
