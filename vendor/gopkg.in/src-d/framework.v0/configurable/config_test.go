package configurable_test

import (
	"fmt"

	"gopkg.in/src-d/framework.v0/configurable"
)

func ExampleBasicConfiguration() {
	type complexConfiguration struct {
		configurable.BasicConfiguration
		Value string `default:"hola"`
	}

	config := &complexConfiguration{}

	configurable.InitConfig(config)

	fmt.Println(config.Value)
	// Output: hola
}
