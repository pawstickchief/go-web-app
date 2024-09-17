package settings

import (
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

var Conf = new(AppConfig)

type AppConfig struct {
	Name         string `mapstructure:"name"`
	Mode         string `mapstructure:"mode"`
	Version      string `mapstructure:"version"`
	Port         int    `mapstructure:"port"`
	StartTime    string `mapstructure:"start_time"`
	MachineId    int64  `mapstructure:"machine_id"`
	ClientUrl    string `mapstructure:"client_url"`
	*LogConfig   `mapstructure:"log"`
	*MySQLConfig `mapstructure:"mysql"`
	*RedisConfig `mapstructure:"redis"`
	*EtcdConfig  `mapstructure:"etcd"`
	*FileConfig  `mapstructure:"file"`
}
type FileConfig struct {
	Filemaxsize int64  `mapstructure:"filemaxsize"`
	Savedir     string `mapstructure:"savedir"`
	Httpurl     string `mapstructure:"httpurl"`
	Httpdir     string `mapstructure:"httpdir"`
}
type LogConfig struct {
	Level      string `mapstructure:"level"`
	Filename   string `mapstructure:"filename"`
	MaxSize    int    `mapstructure:"max_size"`
	MaxAge     int    `mapstructure:"max_age"`
	MaxBackups int    `mapstructure:"max_backups"`
}

type MySQLConfig struct {
	Host         string `mapstructure:"host"`
	Port         int    `mapstructure:"port"`
	User         string `mapstructure:"user"`
	Password     string `mapstructure:"password"`
	DbName       string `mapstructure:"dbname"`
	MaxIdleConns int    `mapstructure:"max_idle_conns"`
	MaxConns     int    `mapstructure:"max_conns"`
}
type RedisConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Password string `mapstructure:"password"`
	DbName   int    `mapstructure:"dbname"`
	PoolSize int    `mapstructure:"pool_size"`
}

type EtcdConfig struct {
	Endpoints   []string
	DialTimeout int
	// 如果需要用户名和密码
	Username string
	Password string
	// 新增 TLS 相关配置
	CaCert     string
	CertFile   string
	KeyFile    string
	ServerName string
}

func Init(configfile string) (err error) {
	viper.SetConfigFile(configfile)
	//指定配置文件
	err = viper.ReadInConfig()
	if err != nil {
		fmt.Printf("Profile read failed, please specify the configuration file:%v\n", err)
		return
	}
	if err := viper.Unmarshal(Conf); err != nil {
		fmt.Printf("viper.Unmarshal failed, err:%v\n", err)
	}
	viper.WatchConfig()
	viper.OnConfigChange(func(in fsnotify.Event) {
		fmt.Println("配置文件修改...")
		if err := viper.Unmarshal(Conf); err != nil {
			fmt.Printf("viper.Unmarshal failed, err:%v\n", err)
		}
	})
	return
}
