package s3plugin_test

import (
	"testing"

	"github.com/greenplum-db/gpbackup-s3-plugin/s3plugin"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestCluster(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "s3_plugin tests")
}

var _ = Describe("s3_plugin tests", func() {
	Context("GetS3Path", func() {
		It("", func() {
			folder := "s3/Dir"
			path := "/tmp/datadir/gpseg-1/backups/20180101/2018010101010101/backup_file"
			newPath := s3plugin.GetS3Path(folder, path)
			expectedPath := "s3/Dir/backups/20180101/2018010101010101/backup_file"
			Expect(newPath).To(Equal(expectedPath))
		})
	})
	Context("ValidateConfig", func() {
		var pluginConfig *s3plugin.PluginConfig
		BeforeEach(func() {
			pluginConfig = &s3plugin.PluginConfig{
				ExecutablePath: "/tmp/location",
				Options: map[string]string{
					"aws_access_key_id":     "12345",
					"aws_secret_access_key": "6789",
					"bucket":                "bucket_name",
					"folder":                "folder_name",
					"region":                "region_name",
					"endpoint":              "endpoint_name",
				},
			}
		})
		It("succeeds when all fields in config filled", func() {
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).To(BeNil())
		})
		It("succeeds when all fields except endpoint filled in config", func() {
			delete(pluginConfig.Options, "endpoint")
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).To(BeNil())
		})
		It("succeeds when all fields except region filled in config", func() {
			delete(pluginConfig.Options, "region")
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).To(BeNil())
		})
		It("sets region to unused when endpoint is used instead of region", func() {
			delete(pluginConfig.Options, "region")
			s3plugin.ValidateConfig(pluginConfig)
			Expect(pluginConfig.Options["region"]).To(Equal("unused"))
		})
		It("returns error when neither region nor endpoint in config", func() {
			delete(pluginConfig.Options, "region")
			delete(pluginConfig.Options, "endpoint")
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).To(HaveOccurred())
		})
		It("returns error when no aws_access_key_id in config", func() {
			delete(pluginConfig.Options, "aws_access_key_id")
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).To(HaveOccurred())
		})
		It("returns error when no aws_secret_access_key in config", func() {
			delete(pluginConfig.Options, "aws_secret_access_key")
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).To(HaveOccurred())
		})
		It("returns error when no bucket in config", func() {
			delete(pluginConfig.Options, "bucket")
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).To(HaveOccurred())
		})
		It("returns error when no folder in config", func() {
			delete(pluginConfig.Options, "folder")
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).To(HaveOccurred())
		})
	})

})
