// Copyright © 2017 Naveego

package main

import (
	_ "github.com/alexbrainman/odbc"
	"github.com/naveego/plugin-odbc/cmd"

	// imported to prevent dep from evicting it, dep doesn't scan magefile.go
	_ "github.com/naveego/ci/go/build"
)

func main() {
	cmd.Execute()
}
