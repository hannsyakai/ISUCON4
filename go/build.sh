#!/bin/bash

go get github.com/go-martini/martini
go get github.com/go-redis/redis
go get github.com/martini-contrib/render
go get github.com/kr/pretty
go get github.com/pkg/profile
go build -o golang-webapp .
