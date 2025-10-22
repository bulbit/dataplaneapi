// Copyright 2019 HAProxy Technologies
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
//

package discovery

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	client_native "github.com/haproxytech/client-native/v6"
	"github.com/haproxytech/client-native/v6/configuration"
	"github.com/haproxytech/client-native/v6/models"
	cn_runtime "github.com/haproxytech/client-native/v6/runtime"

	"github.com/haproxytech/dataplaneapi/haproxy"
	"github.com/haproxytech/dataplaneapi/log"
)

// Using a simple mutex to avoid race conditions when multiple Service Discovery instances are trying to commit
// changes at the same time: need to be refactored.
var mutex = &sync.Mutex{}

// ServiceInstance specifies the needed information required from the service to provide for the ServiceDiscoveryInstance.
type ServiceInstance interface {
	GetName() string
	GetBackendName() string
	GetFrom() string
	Changed() bool
	GetServers() []configuration.ServiceServer
}

type confService struct {
	confService *configuration.Service
	cleanup     bool
}

type discoveryInstanceParams struct {
	LogFields       map[string]interface{}
	SlotsGrowthType string
	Allowlist       []string
	Denylist        []string
	ServerSlotsBase int
	SlotsIncrement  int
}

// ServiceDiscoveryInstance manages and updates all services of a single service discovery.
type ServiceDiscoveryInstance struct {
	client        configuration.Configuration
	haproxyClient client_native.HAProxyClient
	reloadAgent   haproxy.IReloadAgent
	services      map[string]*confService
	serverStates  map[string]map[string]bool // backendName -> serverName -> exists
	transactionID string
	params        discoveryInstanceParams
}

// NewServiceDiscoveryInstance creates a new ServiceDiscoveryInstance.
func NewServiceDiscoveryInstance(client configuration.Configuration, reloadAgent haproxy.IReloadAgent, params discoveryInstanceParams) *ServiceDiscoveryInstance {
	return &ServiceDiscoveryInstance{
		client:       client,
		reloadAgent:  reloadAgent,
		params:       params,
		services:     make(map[string]*confService),
		serverStates: make(map[string]map[string]bool),
	}
}

// UpdateParams updates the scaling params for each service associated with the service discovery.
func (s *ServiceDiscoveryInstance) UpdateParams(params discoveryInstanceParams) error {
	s.params = params
	for _, se := range s.services {
		err := se.confService.UpdateScalingParams(configuration.ScalingParams{
			BaseSlots:       s.params.ServerSlotsBase,
			SlotsGrowthType: s.params.SlotsGrowthType,
			SlotsIncrement:  s.params.SlotsIncrement,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// UpdateServices updates each service dynamically via Runtime API without reloading HAProxy.
// Falls back to configuration file update and reload only if Runtime API fails.
func (s *ServiceDiscoveryInstance) UpdateServices(services []ServiceInstance) error {
	mutex.Lock()
	defer mutex.Unlock()

	// Try to use Runtime API first
	if s.haproxyClient != nil {
		if err := s.updateServicesViaRuntime(services); err != nil {
			s.logWarningf("Runtime API update failed: %s, falling back to config file update", err.Error())
		} else {
			// Successfully updated via Runtime API
			return nil
		}
	}

	// Fallback: traditional config file update with reload
	return s.updateServicesViaConfigFile(services)
}

// updateServicesViaRuntime updates services dynamically using Runtime API
func (s *ServiceDiscoveryInstance) updateServicesViaRuntime(services []ServiceInstance) error {
	runtime, err := s.haproxyClient.Runtime()
	if err != nil {
		return fmt.Errorf("failed to get runtime client: %w", err)
	}

	// Try to get HAProxy version, but continue if it fails since our
	// serializeRuntimeAddServer implementation doesn't actually use it
	haversion, err := runtime.GetVersion()
	if err != nil {
		// Create a default version mimicking 3.2.0 to avoid nil pointer issues
		haversion = cn_runtime.HAProxyVersion{Major: 3, Minor: 2, Patch: 0}
		s.logWarningf("Failed to get HAProxy version, continuing with default (Major=%d, Minor=%d, Patch=%d): %s", haversion.Major, haversion.Minor, haversion.Patch, err.Error())
	} else {
		s.logWarningf("Successfully retrieved HAProxy version: Major=%d, Minor=%d, Patch=%d", haversion.Major, haversion.Minor, haversion.Patch)
	}

	s.markForCleanUp()

	for _, service := range services {
		if s.serviceNotTracked(service.GetName()) {
			continue
		}

		backendName := service.GetBackendName()

		// Initialize backend state tracking if not exists
		if _, ok := s.serverStates[backendName]; !ok {
			s.serverStates[backendName] = make(map[string]bool)
		}

		if !service.Changed() {
			if se, ok := s.services[service.GetName()]; ok {
				se.cleanup = false
			}
			continue
		}

		// Ensure backend exists (this may still require config file update on first run)
		if _, err := s.initService(service); err != nil {
			return fmt.Errorf("failed to init service %s: %w", service.GetName(), err)
		}

		se := s.services[service.GetName()]
		se.cleanup = false

		desiredServers := service.GetServers()
		currentServers := s.serverStates[backendName]

		// Create a map of desired servers
		desiredServerMap := make(map[string]configuration.ServiceServer)
		for _, srv := range desiredServers {
			serverName := s.generateServerName(srv.Address, srv.Port)
			desiredServerMap[serverName] = srv
		}

		// Add new servers and update existing ones
		for serverName, srv := range desiredServerMap {
			if !currentServers[serverName] {
				// Add new server via Runtime API
				if err := s.addServerViaRuntime(runtime, backendName, serverName, srv, &haversion); err != nil {
					s.logErrorf("Failed to add server %s to backend %s: %s", serverName, backendName, err.Error())
					return err
				}
				s.serverStates[backendName][serverName] = true
				log.WithFieldsf(s.params.LogFields, log.InfoLevel, "Added server %s to backend %s via Runtime API", serverName, backendName)
			} else {
				// Server exists, update address if needed via Runtime API
				if err := s.updateServerViaRuntime(runtime, backendName, serverName, srv); err != nil {
					s.logWarningf("Failed to update server %s in backend %s: %s", serverName, backendName, err.Error())
				}
			}
		}

		// Remove servers that no longer exist
		for serverName := range currentServers {
			if _, exists := desiredServerMap[serverName]; !exists {
				if err := s.deleteServerViaRuntime(runtime, backendName, serverName); err != nil {
					s.logWarningf("Failed to delete server %s from backend %s: %s", serverName, backendName, err.Error())
				} else {
					delete(s.serverStates[backendName], serverName)
					log.WithFieldsf(s.params.LogFields, log.InfoLevel, "Deleted server %s from backend %s via Runtime API", serverName, backendName)
				}
			}
		}
	}

	// Note: Cleanup of services no longer tracked is handled separately
	// This requires config file update as we need to remove the entire backend

	return nil
}

// updateServicesViaConfigFile updates services via traditional config file update (fallback)
func (s *ServiceDiscoveryInstance) updateServicesViaConfigFile(services []ServiceInstance) error {
	err := s.startTransaction()
	if err != nil {
		return err
	}
	reload := false
	s.markForCleanUp()
	for _, service := range services {
		if s.serviceNotTracked(service.GetName()) {
			continue
		}
		if !service.Changed() {
			if se, ok := s.services[service.GetName()]; ok {
				se.cleanup = false
			}
			continue
		}
		r, err := s.initService(service)
		if err != nil {
			s.deleteTransaction()
			return err
		}
		reload = reload || r
		se := s.services[service.GetName()]
		r, err = se.confService.Update(service.GetServers())
		if err != nil {
			s.deleteTransaction()
			return err
		}
		reload = reload || r
	}
	r := s.cleanup()
	reload = reload || r
	if reload {
		if err := s.commitTransaction(); err != nil {
			return err
		}
		s.reloadAgent.Reload()
		return nil
	}
	s.deleteTransaction()
	return nil
}

func (s *ServiceDiscoveryInstance) startTransaction() error {
	version, err := s.client.GetVersion("")
	if err != nil {
		return err
	}
	transaction, err := s.client.StartTransaction(version)
	if err != nil {
		return err
	}
	s.transactionID = transaction.ID
	return nil
}

func (s *ServiceDiscoveryInstance) markForCleanUp() {
	for id := range s.services {
		s.services[id].cleanup = true
	}
}

func (s *ServiceDiscoveryInstance) serviceNotTracked(service string) bool {
	if len(s.params.Allowlist) > 0 {
		for _, se := range s.params.Allowlist {
			if se == service {
				return false
			}
		}
		return true
	}
	for _, se := range s.params.Denylist {
		if se == service {
			return true
		}
	}
	return false
}

func (s *ServiceDiscoveryInstance) initService(service ServiceInstance) (bool, error) {
	if se, ok := s.services[service.GetName()]; ok {
		se.confService.SetTransactionID(s.transactionID)
		se.cleanup = false
		return false, nil
	}
	se, err := s.client.NewService(service.GetBackendName(), configuration.ScalingParams{
		BaseSlots:       s.params.ServerSlotsBase,
		SlotsGrowthType: s.params.SlotsGrowthType,
		SlotsIncrement:  s.params.SlotsIncrement,
	})
	if err != nil {
		return false, err
	}
	reload, err := se.Init(s.transactionID, service.GetFrom())
	if err != nil {
		return false, err
	}
	s.services[service.GetName()] = &confService{
		confService: se,
		cleanup:     false,
	}
	return reload, nil
}

func (s *ServiceDiscoveryInstance) cleanup() (reload bool) {
	for service := range s.services {
		if s.services[service].cleanup {
			s.services[service].confService.SetTransactionID(s.transactionID)
			changed, err := s.services[service].confService.Update([]configuration.ServiceServer{})
			if err != nil {
				s.logErrorf("service %s marked for clean-up cannot be updated, %s", service, err.Error())
				continue
			}

			if changed {
				s.logWarningf("service %s marked for clean-up, has not any more backend servers", service)
			}

			reload = reload || changed
		}
	}

	return reload
}

func (s *ServiceDiscoveryInstance) deleteTransaction() {
	if err := s.client.DeleteTransaction(s.transactionID); err != nil {
		s.logWarningf("cannot delete transaction due to an error: %s", err.Error())
	}
	s.transactionID = ""
}

func (s *ServiceDiscoveryInstance) commitTransaction() error {
	_, err := s.client.CommitTransaction(s.transactionID)
	s.transactionID = ""
	return err
}

func (s *ServiceDiscoveryInstance) logWarningf(format string, args ...interface{}) {
	log.WithFieldsf(s.params.LogFields, log.WarnLevel, format, args...)
}

func (s *ServiceDiscoveryInstance) logErrorf(format string, args ...interface{}) {
	log.WithFieldsf(s.params.LogFields, log.ErrorLevel, format, args...)
}

// generateServerName generates a unique server name from address and port
func (s *ServiceDiscoveryInstance) generateServerName(address string, port *int64) string {
	// Sanitize address to be a valid server name (replace dots and colons with dashes)
	sanitized := sanitizeHostname(address)
	if port != nil {
		return fmt.Sprintf("sd-%s-%d", sanitized, *port)
	}
	return fmt.Sprintf("sd-%s", sanitized)
}

// sanitizeHostname replaces invalid characters in hostname for use as server name
func sanitizeHostname(hostname string) string {
	// Replace dots, colons, and other special chars with dashes
	re := regexp.MustCompile(`[^a-zA-Z0-9-]`)
	return re.ReplaceAllString(hostname, "-")
}

// addServerViaRuntime adds a server via Runtime API
func (s *ServiceDiscoveryInstance) addServerViaRuntime(runtime cn_runtime.Runtime, backendName, serverName string, srv configuration.ServiceServer, haversion *cn_runtime.HAProxyVersion) error {
	ras := &models.RuntimeAddServer{
		Name:    serverName,
		Address: srv.Address,
		Port:    srv.Port,
		Check:   "enabled", // Enable health checks by default
	}

	serialized := serializeRuntimeAddServer(ras, haversion)
	return runtime.AddServer(backendName, serverName, serialized)
}

// updateServerViaRuntime updates a server's address via Runtime API
func (s *ServiceDiscoveryInstance) updateServerViaRuntime(runtime cn_runtime.Runtime, backendName, serverName string, srv configuration.ServiceServer) error {
	port := 0
	if srv.Port != nil {
		port = int(*srv.Port)
	}
	return runtime.SetServerAddr(backendName, serverName, srv.Address, port)
}

// deleteServerViaRuntime deletes a server via Runtime API
func (s *ServiceDiscoveryInstance) deleteServerViaRuntime(runtime cn_runtime.Runtime, backendName, serverName string) error {
	// Put server in maintenance before deleting
	if err := runtime.DisableServer(backendName, serverName); err != nil {
		// If server doesn't exist, that's ok
		if !isRuntimeNotFoundError(err) {
			return err
		}
	}
	return runtime.DeleteServer(backendName, serverName)
}

// isRuntimeNotFoundError checks if error is a "not found" error
func isRuntimeNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, "No such server") || strings.Contains(errMsg, "not found")
}

// serializeRuntimeAddServer converts RuntimeAddServer to HAProxy runtime command format
// This is a simplified version for Service Discovery use case
func serializeRuntimeAddServer(srv *models.RuntimeAddServer, haversion *cn_runtime.HAProxyVersion) string {
	var parts []string

	// Address is mandatory
	addr := srv.Address
	if srv.Port != nil {
		addr = fmt.Sprintf("%s:%d", addr, *srv.Port)
	}
	parts = append(parts, addr)

	// Add health check if enabled
	if srv.Check == "enabled" {
		parts = append(parts, "check")
	}

	// Add inter if specified
	if srv.Inter != nil {
		parts = append(parts, fmt.Sprintf("inter %d", *srv.Inter))
	}

	// Return space-separated string with leading space
	return " " + strings.Join(parts, " ")
}
