// +build mage

package main

import (
	"github.com/naveego/dataflow-contracts/plugins"
	"os"

	"github.com/naveego/ci/go/build"
	"github.com/naveego/plugin-odbc/version"
)

func Build() error {
	cfg := build.PluginConfig{
		Package: build.Package{
			VersionString: version.Version.String(),
			PackagePath:   "github.com/naveego/plugin-odbc",
			Name:          "odbc",
			Shrink:        true,
		},
		Targets: []build.PackageTarget{
			build.TargetLinuxAmd64,
			build.TargetDarwinAmd64,
			build.TargetWindowsAmd64,
		},
	}

	err := build.BuildPlugin(cfg)
	return err
}


func PublishBlue() error {
	os.Setenv("UPLOAD", "blue")
	return Build()
}


func GenerateGRPC() error {
	destDir := "./internal/pub"
	return plugins.GeneratePublisher(destDir)
}
