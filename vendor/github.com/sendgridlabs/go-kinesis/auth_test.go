package kinesis

import (
	"os"
	"testing"
)

func TestAuthInterfaceIsImplemented(t *testing.T) {
	var auth Auth = &AuthCredentials{}
	if auth == nil {
		t.Error("Invalid nil auth credentials value")
	}
}

func TestGetSecretKey(t *testing.T) {
	auth := NewAuth("BAD_ACCESS_KEY", "BAD_SECRET_KEY", "BAD_SECURITY_TOKEN")

	if auth.GetAccessKey() != "BAD_ACCESS_KEY" {
		t.Error("incorrect value for auth#accessKey")
	}
}

func TestGetAccessKey(t *testing.T) {
	auth := NewAuth("BAD_ACCESS_KEY", "BAD_SECRET_KEY", "BAD_SECURITY_TOKEN")

	if auth.GetSecretKey() != "BAD_SECRET_KEY" {
		t.Error("incorrect value for auth#secretKey")
	}
}

func TestGetToken(t *testing.T) {
	auth := NewAuth("BAD_ACCESS_KEY", "BAD_SECRET_KEY", "BAD_SECURITY_TOKEN")

	if auth.GetToken() != "BAD_SECURITY_TOKEN" {
		t.Error("incorrect value for auth#token")
	}
}

func TestNewAuthFromEnv(t *testing.T) {
	os.Setenv(AccessEnvKey, "asdf")
	os.Setenv(SecretEnvKey, "asdf2")
	os.Setenv(SecurityTokenEnvKey, "dummy_token")
	// Validate that the fallback environment variables will also work
	defer os.Unsetenv(AccessEnvKey)
	defer os.Unsetenv(SecretEnvKey)
	defer os.Unsetenv(SecurityTokenEnvKey)

	auth, _ := NewAuthFromEnv()

	if auth.GetAccessKey() != "asdf" {
		t.Error("Expected AccessKey to be inferred as \"asdf\"")
	}

	if auth.GetSecretKey() != "asdf2" {
		t.Error("Expected SecretKey to be inferred as \"asdf2\"")
	}

	if auth.GetToken() != "dummy_token" {
		t.Error("Expected SecretKey to be inferred as \"dummy_token\"")
	}
}

func TestNewAuthFromEnvWithFallbackVars(t *testing.T) {
	os.Setenv(AccessEnvKeyId, "asdf")
	os.Setenv(SecretEnvAccessKey, "asdf2")
	os.Setenv(SecurityTokenEnvKey, "dummy_token")
	defer os.Unsetenv(AccessEnvKey)
	defer os.Unsetenv(SecretEnvKey)
	defer os.Unsetenv(SecurityTokenEnvKey)

	auth, _ := NewAuthFromEnv()

	if auth.GetAccessKey() != "asdf" {
		t.Error("Expected AccessKey to be inferred as \"asdf\"")
	}

	if auth.GetSecretKey() != "asdf2" {
		t.Error("Expected SecretKey to be inferred as \"asdf2\"")
	}
}
