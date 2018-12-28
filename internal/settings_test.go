package internal_test

import (
	. "github.com/naveego/plugin-odbc/internal"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Settings", func() {

	var (
		settings Settings
	)

	BeforeEach(func() {
		settings = *GetTestSettings()
	})

	Describe("Validate", func() {

		It("Should error if connectionString is not set", func() {
			settings.ConnectionString = ""
			Expect(settings.Validate()).ToNot(Succeed())
		})

		It("Should error if password is not set", func() {
			settings.Password = ""
			Expect(settings.Validate()).ToNot(Succeed())
		})

		It("Should succeed if settings are valid for sql", func() {
			Expect(settings.Validate()).To(Succeed())
		})
	})
})
