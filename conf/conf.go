package conf

import (
	"io/ioutil"
	"os"
	"path/filepath"

	yaml "gopkg.in/yaml.v2"
)

type Conf struct {
	Kafka KafkaConf `yaml:"kafka"`
}

type KafkaConf struct {
	Host    string   `yaml:"host"`
	Addrs   []string `yaml:"addrs"`
	PartNum int      `yaml:"partNum"` // 分区数
	Group   struct {
		Delay string `yaml:"delay"`
	} `yaml:"group"`
	Topic struct {
		Delay5s  string `yaml:"delay5s"`
		Delay1m  string `yaml:"delay1m"`
		Delay10m string `yaml:"delay10m"`
		Delay1h  string `yaml:"delay1h"`
	} `yaml:"topic"`
}

var (
	_m = Conf{}
)

func Get() *Conf {
	return &_m
}

// LoadConfFile load confile.yaml
func LoadConfFile() (err error) {
	basePath, err := os.Getwd()
	if err != nil {
		return err
	}
	fileName := filepath.Join(basePath, "config.yaml")
	yamlFile, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(yamlFile, &_m)
	if err != nil {
		return err
	}
	return
}
