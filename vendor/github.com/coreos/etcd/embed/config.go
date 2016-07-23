// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embed

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"strings"

	"github.com/coreos/etcd/discovery"
	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/pkg/cors"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/coreos/etcd/pkg/types"
	"github.com/ghodss/yaml"
)

const (
	ClusterStateFlagNew      = "new"
	ClusterStateFlagExisting = "existing"

	DefaultName                     = "default"
	DefaultInitialAdvertisePeerURLs = "http://localhost:2380"
	DefaultAdvertiseClientURLs      = "http://localhost:2379"
	DefaultListenPeerURLs           = "http://localhost:2380"
	DefaultListenClientURLs         = "http://localhost:2379"
	DefaultMaxSnapshots             = 5
	DefaultMaxWALs                  = 5

	// maxElectionMs specifies the maximum value of election timeout.
	// More details are listed in ../Documentation/tuning.md#time-parameters.
	maxElectionMs = 50000
)

var (
	ErrConflictBootstrapFlags = fmt.Errorf("multiple discovery or bootstrap flags are set. " +
		"Choose one of \"initial-cluster\", \"discovery\" or \"discovery-srv\"")
	ErrUnsetAdvertiseClientURLsFlag = fmt.Errorf("--advertise-client-urls is required when --listen-client-urls is set explicitly")
)

// Config holds the arguments for configuring an etcd server.
type Config struct {
	// member

	CorsInfo                *cors.CORSInfo
	LPUrls, LCUrls          []url.URL
	Dir                     string `json:"data-dir"`
	WalDir                  string `json:"wal-dir"`
	MaxSnapFiles            uint   `json:"max-snapshots"`
	MaxWalFiles             uint   `json:"max-wals"`
	Name                    string `json:"name"`
	SnapCount               uint64 `json:"snapshot-count"`
	AutoCompactionRetention int    `json:"auto-compaction-retention"`

	// TickMs is the number of milliseconds between heartbeat ticks.
	// TODO: decouple tickMs and heartbeat tick (current heartbeat tick = 1).
	// make ticks a cluster wide configuration.
	TickMs            uint  `json:"heartbeat-interval"`
	ElectionMs        uint  `json:"election-timeout"`
	QuotaBackendBytes int64 `json:"quota-backend-bytes"`

	// clustering

	APUrls, ACUrls      []url.URL
	ClusterState        string `json:"initial-cluster-state"`
	DNSCluster          string `json:"discovery-srv"`
	Dproxy              string `json:"discovery-proxy"`
	Durl                string `json:"discovery"`
	InitialCluster      string `json:"initial-cluster"`
	InitialClusterToken string `json:"initial-cluster-token"`
	StrictReconfigCheck bool   `json:"strict-reconfig-check"`

	// security

	ClientTLSInfo transport.TLSInfo
	ClientAutoTLS bool
	PeerTLSInfo   transport.TLSInfo
	PeerAutoTLS   bool

	// debug

	Debug        bool   `json:"debug"`
	LogPkgLevels string `json:"log-package-levels"`
	EnablePprof  bool

	// ForceNewCluster starts a new cluster even if previously started; unsafe.
	ForceNewCluster bool `json:"force-new-cluster"`
}

// configYAML holds the config suitable for yaml parsing
type configYAML struct {
	Config
	configJSON
}

// configJSON has file options that are translated into Config options
type configJSON struct {
	LPUrlsJSON         string         `json:"listen-peer-urls"`
	LCUrlsJSON         string         `json:"listen-client-urls"`
	CorsJSON           string         `json:"cors"`
	APUrlsJSON         string         `json:"initial-advertise-peer-urls"`
	ACUrlsJSON         string         `json:"advertise-client-urls"`
	ClientSecurityJSON securityConfig `json:"client-transport-security"`
	PeerSecurityJSON   securityConfig `json:"peer-transport-security"`
}

type securityConfig struct {
	CAFile        string `json:"ca-file"`
	CertFile      string `json:"cert-file"`
	KeyFile       string `json:"key-file"`
	CertAuth      bool   `json:"client-cert-auth"`
	TrustedCAFile string `json:"trusted-ca-file"`
	AutoTLS       bool   `json:"auto-tls"`
}

// NewConfig creates a new Config populated with default values.
func NewConfig() *Config {
	apurl, _ := url.Parse(DefaultInitialAdvertisePeerURLs)
	acurl, _ := url.Parse(DefaultAdvertiseClientURLs)
	cfg := &Config{
		CorsInfo:            &cors.CORSInfo{},
		MaxSnapFiles:        DefaultMaxSnapshots,
		MaxWalFiles:         DefaultMaxWALs,
		Name:                DefaultName,
		SnapCount:           etcdserver.DefaultSnapCount,
		TickMs:              100,
		ElectionMs:          1000,
		APUrls:              []url.URL{*apurl},
		ACUrls:              []url.URL{*acurl},
		ClusterState:        ClusterStateFlagNew,
		InitialClusterToken: "etcd-cluster",
	}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	return cfg
}

func ConfigFromFile(path string) (*Config, error) {
	cfg := &configYAML{}
	if err := cfg.configFromFile(path); err != nil {
		return nil, err
	}
	return &cfg.Config, nil
}

func (cfg *configYAML) configFromFile(path string) error {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(b, cfg)
	if err != nil {
		return err
	}

	if cfg.LPUrlsJSON != "" {
		u, err := types.NewURLs(strings.Split(cfg.LPUrlsJSON, ","))
		if err != nil {
			plog.Fatalf("unexpected error setting up listen-peer-urls: %v", err)
		}
		cfg.LPUrls = []url.URL(u)
	}

	if cfg.LCUrlsJSON != "" {
		u, err := types.NewURLs(strings.Split(cfg.LCUrlsJSON, ","))
		if err != nil {
			plog.Fatalf("unexpected error setting up listen-client-urls: %v", err)
		}
		cfg.LCUrls = []url.URL(u)
	}

	if cfg.CorsJSON != "" {
		if err := cfg.CorsInfo.Set(cfg.CorsJSON); err != nil {
			plog.Panicf("unexpected error setting up cors: %v", err)
		}
	}

	if cfg.APUrlsJSON != "" {
		u, err := types.NewURLs(strings.Split(cfg.APUrlsJSON, ","))
		if err != nil {
			plog.Fatalf("unexpected error setting up initial-advertise-peer-urls: %v", err)
		}
		cfg.APUrls = []url.URL(u)
	}

	if cfg.ACUrlsJSON != "" {
		u, err := types.NewURLs(strings.Split(cfg.ACUrlsJSON, ","))
		if err != nil {
			plog.Fatalf("unexpected error setting up advertise-peer-urls: %v", err)
		}
		cfg.ACUrls = []url.URL(u)
	}

	if cfg.ClusterState == "" {
		cfg.ClusterState = ClusterStateFlagNew
	}

	copySecurityDetails := func(tls *transport.TLSInfo, ysc *securityConfig) {
		tls.CAFile = ysc.CAFile
		tls.CertFile = ysc.CertFile
		tls.KeyFile = ysc.KeyFile
		tls.ClientCertAuth = ysc.CertAuth
		tls.TrustedCAFile = ysc.TrustedCAFile
	}
	copySecurityDetails(&cfg.ClientTLSInfo, &cfg.ClientSecurityJSON)
	copySecurityDetails(&cfg.PeerTLSInfo, &cfg.PeerSecurityJSON)
	cfg.ClientAutoTLS = cfg.ClientSecurityJSON.AutoTLS
	cfg.PeerAutoTLS = cfg.PeerSecurityJSON.AutoTLS

	return cfg.Validate()
}

func (cfg *Config) Validate() error {
	// Check if conflicting flags are passed.
	nSet := 0
	for _, v := range []bool{cfg.Durl != "", cfg.InitialCluster != "", cfg.DNSCluster != ""} {
		if v {
			nSet++
		}
	}

	if cfg.ClusterState != ClusterStateFlagNew && cfg.ClusterState != ClusterStateFlagExisting {
		return fmt.Errorf("unexpected clusterState %q", cfg.ClusterState)
	}

	if nSet > 1 {
		return ErrConflictBootstrapFlags
	}

	if 5*cfg.TickMs > cfg.ElectionMs {
		return fmt.Errorf("--election-timeout[%vms] should be at least as 5 times as --heartbeat-interval[%vms]", cfg.ElectionMs, cfg.TickMs)
	}
	if cfg.ElectionMs > maxElectionMs {
		return fmt.Errorf("--election-timeout[%vms] is too long, and should be set less than %vms", cfg.ElectionMs, maxElectionMs)
	}

	// check this last since proxying in etcdmain may make this OK
	if cfg.LCUrls != nil && cfg.ACUrls == nil {
		return ErrUnsetAdvertiseClientURLsFlag
	}

	return nil
}

// PeerURLsMapAndToken sets up an initial peer URLsMap and cluster token for bootstrap or discovery.
func (cfg *Config) PeerURLsMapAndToken(which string) (urlsmap types.URLsMap, token string, err error) {
	switch {
	case cfg.Durl != "":
		urlsmap = types.URLsMap{}
		// If using discovery, generate a temporary cluster based on
		// self's advertised peer URLs
		urlsmap[cfg.Name] = cfg.APUrls
		token = cfg.Durl
	case cfg.DNSCluster != "":
		var clusterStr string
		clusterStr, token, err = discovery.SRVGetCluster(cfg.Name, cfg.DNSCluster, cfg.InitialClusterToken, cfg.APUrls)
		if err != nil {
			return nil, "", err
		}
		urlsmap, err = types.NewURLsMap(clusterStr)
		// only etcd member must belong to the discovered cluster.
		// proxy does not need to belong to the discovered cluster.
		if which == "etcd" {
			if _, ok := urlsmap[cfg.Name]; !ok {
				return nil, "", fmt.Errorf("cannot find local etcd member %q in SRV records", cfg.Name)
			}
		}
	default:
		// We're statically configured, and cluster has appropriately been set.
		urlsmap, err = types.NewURLsMap(cfg.InitialCluster)
		token = cfg.InitialClusterToken
	}
	return urlsmap, token, err
}

func (cfg Config) InitialClusterFromName(name string) (ret string) {
	if len(cfg.APUrls) == 0 {
		return ""
	}
	n := name
	if name == "" {
		n = DefaultName
	}
	for i := range cfg.APUrls {
		ret = ret + "," + n + "=" + cfg.APUrls[i].String()
	}
	return ret[1:]
}

func (cfg Config) IsNewCluster() bool { return cfg.ClusterState == ClusterStateFlagNew }
func (cfg Config) ElectionTicks() int { return int(cfg.ElectionMs / cfg.TickMs) }
