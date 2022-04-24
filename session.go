package session_redis

import (
	"errors"
	"time"

	. "github.com/chefsgo/base"
	"github.com/chefsgo/chef"
	"github.com/chefsgo/log"
	"github.com/chefsgo/session"
	"github.com/chefsgo/util"
	"github.com/gomodule/redigo/redis"
)

var (
	errInvalidConnection = errors.New("Invalid connection")
	errNotFound          = errors.New("NotFound")
)

type (
	redisDriver  struct{}
	redisConnect struct {
		name    string
		config  session.Config
		setting redisSetting

		client *redis.Pool
	}
	//配置文件
	redisSetting struct {
		Server   string //服务器地址，ip:端口
		Password string //服务器auth密码
		Database string //数据库

		Idle    int //最大空闲连接
		Active  int //最大激活连接，同时最大并发
		Timeout time.Duration
	}
)

//连接
func (driver *redisDriver) Connect(name string, config session.Config) (session.Connect, error) {
	//获取配置信息
	setting := redisSetting{
		Server: "127.0.0.1:6379", Password: "", Database: "",
		Idle: 30, Active: 100, Timeout: 240,
	}

	if vv, ok := config.Setting["server"].(string); ok && vv != "" {
		setting.Server = vv
	}
	if vv, ok := config.Setting["password"].(string); ok && vv != "" {
		setting.Password = vv
	}

	//数据库，redis的0-16号
	if v, ok := config.Setting["database"].(string); ok {
		setting.Database = v
	}

	if vv, ok := config.Setting["idle"].(int64); ok && vv > 0 {
		setting.Idle = int(vv)
	}
	if vv, ok := config.Setting["active"].(int64); ok && vv > 0 {
		setting.Active = int(vv)
	}
	if vv, ok := config.Setting["timeout"].(int64); ok && vv > 0 {
		setting.Timeout = time.Second * time.Duration(vv)
	}
	if vv, ok := config.Setting["timeout"].(string); ok && vv != "" {
		td, err := util.ParseDuration(vv)
		if err == nil {
			setting.Timeout = td
		}
	}

	return &redisConnect{
		name: name, config: config, setting: setting,
	}, nil
}

//打开连接
func (connect *redisConnect) Open() error {
	connect.client = &redis.Pool{
		MaxIdle: connect.setting.Idle, MaxActive: connect.setting.Active, IdleTimeout: connect.setting.Timeout,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", connect.setting.Server)
			if err != nil {
				log.Warning("session.redis.dial", err)
				return nil, err
			}

			//如果有验证
			if connect.setting.Password != "" {
				if _, err := c.Do("AUTH", connect.setting.Password); err != nil {
					c.Close()
					log.Warning("session.redis.auth", err)
					return nil, err
				}
			}
			//如果指定库
			if connect.setting.Database != "" {
				if _, err := c.Do("SELECT", connect.setting.Database); err != nil {
					c.Close()
					log.Warning("session.redis.select", err)
					return nil, err
				}
			}

			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	//打开一个试一下
	conn := connect.client.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}
	return nil
}

//关闭连接
func (connect *redisConnect) Close() error {
	if connect.client != nil {
		if err := connect.client.Close(); err != nil {
			return err

		}
	}
	return nil
}

//查询会话，
func (connect *redisConnect) Read(id string) (Map, error) {
	if connect.client == nil {
		return nil, errInvalidConnection
	}

	conn := connect.client.Get()
	defer conn.Close()

	val, err := redis.String(conn.Do("GET", id))
	if err != nil {
		return nil, err
	}

	m := Map{}
	err = chef.UnmarshalJSON([]byte(val), &m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

//更新会话
func (connect *redisConnect) Write(id string, value Map, expiry time.Duration) error {
	if connect.client == nil {
		return errInvalidConnection
	}

	conn := connect.client.Get()
	defer conn.Close()

	bytes, err := chef.MarshalJSON(value)
	if err != nil {
		return err
	}

	if expiry <= expiry {
		expiry = connect.config.Expiry
	}

	args := []Any{
		id, string(bytes),
	}
	if expiry > 0 {
		args = append(args, "EX", int(expiry.Seconds()))
	}

	_, err = conn.Do("SET", args...)
	if err != nil {
		return err
	}
	return nil
}

//删除会话
func (connect *redisConnect) Delete(id string) error {
	if connect.client == nil {
		return errInvalidConnection
	}
	conn := connect.client.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", id)
	if err != nil {
		return err
	}

	return nil
}

//删除会话
func (connect *redisConnect) Clear(prefix string) error {
	if connect.client == nil {
		return errInvalidConnection
	}
	conn := connect.client.Get()
	defer conn.Close()

	keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
	if err != nil {
		return err
	}

	for _, key := range keys {
		_, err := conn.Do("DEL", key)
		if err != nil {
			return err
		}
	}

	return nil
}
