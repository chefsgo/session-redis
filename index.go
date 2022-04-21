package session_redis

import (
	"github.com/chefsgo/chef"
)

func Driver() chef.SessionDriver {
	return &redisSessionDriver{}
}

func init() {
	chef.Register("redis", Driver())
}
