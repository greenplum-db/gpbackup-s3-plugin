package s3plugin_test

import (
	"flag"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup-s3-plugin/s3plugin"
	"github.com/urfave/cli"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestCluster(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "s3_plugin tests")
}

var _ = Describe("s3_plugin tests", func() {
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
	Describe("GetS3Path", func() {
		It("it combines the folder directory with a path that results from removing all but the last 3 directories of the file path parameter", func() {
			folder := "s3/Dir"
			path := "/a/b/c/tmp/datadir/gpseg-1/backups/20180101/20180101082233/backup_file"
			newPath := s3plugin.GetS3Path(folder, path)
			expectedPath := "s3/Dir/backups/20180101/20180101082233/backup_file"
			Expect(newPath).To(Equal(expectedPath))
		})
	})
	Describe("ShouldEnableDebug", func() {
		It("returns INFO when no debug in config during gpbackup", func() {
			s3plugin.ShouldEnableDebug(pluginConfig, s3plugin.Gpbackup)
			result := s3plugin.GetDebug()
			Expect(result).To(Equal(gplog.LOGINFO))
		})
		It("returns INFO when no debug in config during gprestore", func() {
			s3plugin.ShouldEnableDebug(pluginConfig, s3plugin.Gprestore)
			result := s3plugin.GetDebug()
			Expect(result).To(Equal(gplog.LOGINFO))
		})
		It("returns DEBUG when debug set to 'on' in config", func() {
			pluginConfig.Options["debug"] = "on"
			s3plugin.ShouldEnableDebug(pluginConfig, s3plugin.Gpbackup)
			result := s3plugin.GetDebug()
			Expect(result).To(Equal(gplog.LOGDEBUG))
		})
		It("returns INFO when debug set to 'off' in config", func() {
			pluginConfig.Options["debug"] = "off"
			s3plugin.ShouldEnableDebug(pluginConfig, s3plugin.Gprestore)
			result := s3plugin.GetDebug()
			Expect(result).To(Equal(gplog.LOGINFO))
		})
		It("returns INFO when debug set to anything else in config", func() {
			pluginConfig.Options["debug"] = "random_text"
			s3plugin.ShouldEnableDebug(pluginConfig, s3plugin.Gpbackup)
			result := s3plugin.GetDebug()
			Expect(result).To(Equal(gplog.LOGINFO))
		})
	})
	Describe("ShouldEnableEncryption", func() {
		It("returns true when no encryption in config", func() {
			delete(pluginConfig.Options, "encryption")
			result := s3plugin.ShouldEnableEncryption(pluginConfig)
			Expect(result).To(BeTrue())
		})
		It("returns true when encryption set to 'on' in config", func() {
			pluginConfig.Options["encryption"] = "on"
			result := s3plugin.ShouldEnableEncryption(pluginConfig)
			Expect(result).To(BeTrue())
		})
		It("returns false when encryption set to 'off' in config", func() {
			pluginConfig.Options["encryption"] = "off"
			result := s3plugin.ShouldEnableEncryption(pluginConfig)
			Expect(result).To(BeFalse())
		})
		It("returns true when encryption set to anything else in config", func() {
			pluginConfig.Options["encryption"] = "random_text"
			result := s3plugin.ShouldEnableEncryption(pluginConfig)
			Expect(result).To(BeTrue())
		})
	})
	Describe("ValidateConfig", func() {
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
		It("succeeds when all fields except aws_access_key_id and aws_secret_access_key in config", func() {
			delete(pluginConfig.Options, "aws_access_key_id")
			delete(pluginConfig.Options, "aws_secret_access_key")
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).To(BeNil())
		})
		It("sets region to unused when endpoint is used instead of region", func() {
			delete(pluginConfig.Options, "region")
			err := s3plugin.ValidateConfig(pluginConfig)
			Expect(err).ToNot(HaveOccurred())
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
	Describe("Delete", func() {
		var flags *flag.FlagSet

		BeforeEach(func() {
			flags = flag.NewFlagSet("testing flagset", flag.PanicOnError)

			options := make(map[string]string, 0)
			options["region"] = "us-west-2"
			options["aws_access_key_id"] = "myId"
			options["aws_secret_access_key"] = "secret"
			options["bucket"] = "myBucket"
			options["folder"] = "foo/bar"
		})
		It("returns error when timestamp is not provided", func() {
			err := flags.Parse([]string{"myconfigfilepath"})
			Expect(err).ToNot(HaveOccurred())
			context := cli.NewContext(nil, flags, nil)

			err = s3plugin.DeleteBackup(context)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("delete requires a <timestamp>"))
		})
		It("returns error when timestamp does not parse", func() {
			err := flags.Parse([]string{"myconfigfilepath", "badformat"})
			Expect(err).ToNot(HaveOccurred())
			context := cli.NewContext(nil, flags, nil)

			err = s3plugin.DeleteBackup(context)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("delete requires a <timestamp> with format YYYYMMDDHHMMSS, but received: badformat"))
		})
	})
})
