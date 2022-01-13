module internal/log

go 1.17

replace api/v1/api/log_v1 => ../../api/v1/api/log_v1

require (
	api/v1/api/log_v1 v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.7.0
	github.com/tysonmote/gommap v0.0.1
	google.golang.org/protobuf v1.27.1
)

require (
	github.com/davecgh/go-spew v1.1.0 // indirect
	github.com/golang/protobuf v1.5.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.0.0-20190311183353-d8887717615a // indirect
	golang.org/x/sys v0.0.0-20190215142949-d0b11bdaac8a // indirect
	golang.org/x/text v0.3.0 // indirect
	google.golang.org/genproto v0.0.0-20190819201941-24fa4b261c55 // indirect
	google.golang.org/grpc v1.32.0 // indirect
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c // indirect
)
