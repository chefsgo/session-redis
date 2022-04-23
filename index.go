package session_redis

import (
	"github.com/chefsgo/session"
)

func Driver() session.Driver {
	return &redisDriver{}
}

func init() {
	session.Register("redis", Driver())
}
