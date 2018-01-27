package configurable_test

import (
	"fmt"
	"os"

	"gopkg.in/src-d/framework.v0/configurable"
)

func ExampleInitConfig_initializesFieldWithoutDefault() {
	type mockTestConfig struct {
		configurable.BasicConfiguration
		MyFieldWithoutDefault string
	}

	config := &mockTestConfig{}

	configurable.InitConfig(config)

	fmt.Println(config.MyFieldWithoutDefault)
	// Output:
}

func ExampleInitConfig_initializesFieldWithDefault() {
	type mockTestConfig struct {
		configurable.BasicConfiguration
		MyFieldWithDefault string `default:"mydefault"`
	}

	config := &mockTestConfig{}

	configurable.InitConfig(config)

	fmt.Println(config.MyFieldWithDefault)
	// Output: mydefault
}

func ExampleInitConfig_initializesFieldFromEnvironment() {
	type mockTestConfig struct {
		configurable.BasicConfiguration
		MyFieldFromEnv string `envconfig:"MY_ENV_VAR"`
	}

	expectedString := "my expected string"
	os.Setenv("MY_ENV_VAR", expectedString)
	defer os.Unsetenv("MY_ENV_VAR")
	config := &mockTestConfig{}

	configurable.InitConfig(config)

	fmt.Println(config.MyFieldFromEnv)
	// Output: my expected string
}

func ExampleInitConfig_environmentValueWinsEvenIfThereIsAlsoDefault() {
	type mockTestConfig struct {
		configurable.BasicConfiguration
		MyFieldFromEnvWithDefault string `envconfig:"MY_WINNING_ENV_VAR" default:"defaulted"`
	}

	expectedString := "my expected string"
	os.Setenv("MY_WINNING_ENV_VAR", expectedString)
	defer os.Unsetenv("MY_WINNING_ENV_VAR")
	config := &mockTestConfig{}

	configurable.InitConfig(config)

	fmt.Println(config.MyFieldFromEnvWithDefault)
	// Output: my expected string
}

func ExampleInitConfig_defaultIsAppliedIfThereIsNoEnvVar() {
	type mockTestConfig struct {
		configurable.BasicConfiguration
		MyFieldFromEnvWithDefault string `envconfig:"MY_WINNING_ENV_VAR" default:"defaulted"`
	}

	os.Unsetenv("MY_WINNING_ENV_VAR")
	config := &mockTestConfig{}

	configurable.InitConfig(config)

	fmt.Println(config.MyFieldFromEnvWithDefault)
	// Output: defaulted
}
