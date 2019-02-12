package env

import (
	"log"
	"os"
	"strconv"
)

func MustGetEnv(k string) string {
	v, ok := os.LookupEnv(k)
	if !ok {
		log.Fatalf("env var '%s' not set", k)
	}

	return v
}

func MustGetIntEnv(k string) int {
	v, err := strconv.Atoi(MustGetEnv(k))
	if err != nil {
		log.Fatalf("env var '%s' is not an integer: %q", k, err)
	}

	return v
}
