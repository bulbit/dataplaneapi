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

// updateServicesViaRuntime updates services dynamically using Runtime API for immediate effect
// and also persists changes to the configuration file to survive HAProxy reloads.
// This approach provides:
// 1. Immediate updates via Runtime API (no reload needed)
// 2. Persistent configuration in haproxy.cfg for true reload scenarios
// 3. Conditional updates - only updates servers when there are actual changes
func (s *ServiceDiscoveryInstance) updateServicesViaRuntime(services []ServiceInstance) error {
	runtime, err := s.haproxyClient.Runtime()
	if err != nil {
		return fmt.Errorf("failed to get runtime client: %w", err)
	}

	// Log socket path for debugging
	socketPath := runtime.SocketPath()
	isStatsSocket := runtime.IsStatsSocket()
	s.logWarningf("Runtime API using socket: %s (isStatsSocket: %t)", socketPath, isStatsSocket)

	// Initialize serverStates by reading current runtime state from HAProxy
	// This ensures we track existing servers (including those from config file)
	for _, service := range services {
		if s.serviceNotTracked(service.GetName()) {
			continue
		}
		backendName := service.GetBackendName()
		if _, ok := s.serverStates[backendName]; !ok {
			s.serverStates[backendName] = make(map[string]bool)
			// Read existing servers from HAProxy runtime
			existingServers, err := runtime.GetServersState(backendName)
			if err != nil {
				s.logWarningf("Failed to get existing servers for backend %s: %s", backendName, err.Error())
			} else {
				for _, srv := range existingServers {
					s.serverStates[backendName][srv.Name] = true
					s.logWarningf("Found existing server %s in backend %s (address: %s)", srv.Name, backendName, srv.Address)
				}
			}
		}
	}

	// Try to get HAProxy version
	haversion, err := runtime.GetVersion()
	if err != nil {
		s.logWarningf("Failed to get HAProxy version, continuing with default version (Major=%d, Minor=%d, Patch=%d): %s", haversion.Major, haversion.Minor, haversion.Patch, err.Error())
	}

	// Start a configuration transaction to persist changes to config file
	if err := s.startTransaction(); err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}

	s.markForCleanUp()

	configChanged := false

	for _, service := range services {
		if s.serviceNotTracked(service.GetName()) {
			continue
		}

		backendName := service.GetBackendName()

		if !service.Changed() {
			if se, ok := s.services[service.GetName()]; ok {
				se.cleanup = false
			}
			continue
		}

		// Ensure backend exists (this may still require config file update on first run)
		if _, err := s.initService(service); err != nil {
			s.deleteTransaction()
			return fmt.Errorf("failed to init service %s: %w", service.GetName(), err)
		}

		se := s.services[service.GetName()]
		se.cleanup = false

		desiredServers := service.GetServers()

		// Get current runtime state for this backend to match by address
		currentRuntimeServers, err := runtime.GetServersState(backendName)
		if err != nil {
			s.logWarningf("Failed to get current servers for backend %s: %s", backendName, err.Error())
			currentRuntimeServers = nil
		}

		// Build maps for comparison: address:port -> server (runtime) and desired servers
		currentServersByAddr := make(map[string]*models.RuntimeServer) // "ip:port" -> RuntimeServer
		for _, srv := range currentRuntimeServers {
			addr := srv.Address
			if srv.Port != nil {
				addr = fmt.Sprintf("%s:%d", srv.Address, *srv.Port)
			}
			currentServersByAddr[addr] = srv
		}

		desiredServersByAddr := make(map[string]configuration.ServiceServer) // "ip:port" -> ServiceServer
		for _, srv := range desiredServers {
			addr := srv.Address
			if srv.Port != nil {
				addr = fmt.Sprintf("%s:%d", srv.Address, *srv.Port)
			}
			desiredServersByAddr[addr] = srv
		}

		// Add new servers that don't exist yet
		for addr, srv := range desiredServersByAddr {
			if existingServer, exists := currentServersByAddr[addr]; !exists {
				// Server doesn't exist, add it
				serverName := s.generateServerName(srv.Address, srv.Port)
				if err := s.addServerViaRuntime(runtime, backendName, serverName, srv, &haversion); err != nil {
					s.logErrorf("Failed to add server %s (%s) to backend %s: %s", serverName, addr, backendName, err.Error())
					s.deleteTransaction()
					return err
				}
				log.WithFieldsf(s.params.LogFields, log.InfoLevel, "Added and enabled server %s (%s) to backend %s via Runtime API", serverName, addr, backendName)
			} else {
				// Server exists with this address, check if update is needed
				if s.serverNeedsUpdate(existingServer, srv) {
					if err := s.updateServerViaRuntime(runtime, backendName, existingServer.Name, srv); err != nil {
						s.logWarningf("Failed to update server %s (%s) in backend %s: %s", existingServer.Name, addr, backendName, err.Error())
					} else {
						log.WithFieldsf(s.params.LogFields, log.InfoLevel, "Updated server %s (%s) in backend %s via Runtime API", existingServer.Name, addr, backendName)
					}
				}
			}
		}

		// Remove servers that no longer exist in desired state
		for addr, server := range currentServersByAddr {
			if _, exists := desiredServersByAddr[addr]; !exists {
				if err := s.deleteServerViaRuntime(runtime, backendName, server.Name); err != nil {
					s.logWarningf("Failed to delete server %s (%s) from backend %s: %s", server.Name, addr, backendName, err.Error())
				} else {
					log.WithFieldsf(s.params.LogFields, log.InfoLevel, "Deleted server %s (%s) from backend %s via Runtime API", server.Name, addr, backendName)
				}
			}
		}

		// Update the configuration file with the desired servers
		changed, err := se.confService.Update(desiredServers)
		if err != nil {
			s.deleteTransaction()
			return fmt.Errorf("failed to update config for service %s: %w", service.GetName(), err)
		}
		configChanged = configChanged || changed
	}

	// Cleanup services no longer tracked
	cleanupChanged := s.cleanup()
	configChanged = configChanged || cleanupChanged

	// Commit the configuration transaction if there were changes
	if configChanged {
		if err := s.commitTransaction(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		log.WithFieldsf(s.params.LogFields, log.InfoLevel, "Configuration file updated with runtime changes")
	} else {
		s.deleteTransaction()
	}

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

// serverNeedsUpdate checks if a server's configuration has changed and needs updating
func (s *ServiceDiscoveryInstance) serverNeedsUpdate(current *models.RuntimeServer, desired configuration.ServiceServer) bool {
	// Check if address changed
	if current.Address != desired.Address {
		return true
	}

	// Check if port changed
	currentPort := 0
	if current.Port != nil {
		currentPort = int(*current.Port)
	}
	desiredPort := 0
	if desired.Port != nil {
		desiredPort = int(*desired.Port)
	}
	if currentPort != desiredPort {
		return true
	}

	// Add more checks here if needed for other server properties
	// For now, we only check address and port since those are the main fields
	// that service discovery typically manages

	return false
}

// getBackendHealthCheckDefaults retrieves health check defaults from backend's default-server directive
func (s *ServiceDiscoveryInstance) getBackendHealthCheckDefaults(backendName string) (inter, rise, fall *int64) {
	// Default fallback values (HAProxy defaults)
	defaultInter := int64(2000) // 2 seconds
	defaultRise := int64(2)     // 2 successful checks
	defaultFall := int64(3)     // 3 failed checks

	// Try to get backend configuration
	_, backend, err := s.client.GetBackend(backendName, "")
	if err != nil {
		// If we can't get backend config, use defaults
		return &defaultInter, &defaultRise, &defaultFall
	}

	// Read from backend's default-server directive if available
	if backend.DefaultServer != nil {
		// Get inter (check interval) from default-server
		if backend.DefaultServer.Inter != nil && *backend.DefaultServer.Inter > 0 {
			inter = backend.DefaultServer.Inter
		}

		// Get rise (consecutive successful checks before UP) from default-server
		if backend.DefaultServer.Rise != nil && *backend.DefaultServer.Rise > 0 {
			rise = backend.DefaultServer.Rise
		}

		// Get fall (consecutive failed checks before DOWN) from default-server
		if backend.DefaultServer.Fall != nil && *backend.DefaultServer.Fall > 0 {
			fall = backend.DefaultServer.Fall
		}
	}

	// Fall back to hardcoded defaults if not specified in backend
	if inter == nil {
		inter = &defaultInter
	}
	if rise == nil {
		rise = &defaultRise
	}
	if fall == nil {
		fall = &defaultFall
	}

	return inter, rise, fall
}

// addServerViaRuntime adds a server via Runtime API and enables it
func (s *ServiceDiscoveryInstance) addServerViaRuntime(runtime cn_runtime.Runtime, backendName, serverName string, srv configuration.ServiceServer, haversion *cn_runtime.HAProxyVersion) error {
	// Get health check defaults from backend configuration
	inter, rise, fall := s.getBackendHealthCheckDefaults(backendName)

	ras := &models.RuntimeAddServer{
		Name:    serverName,
		Address: srv.Address,
		Port:    srv.Port,
		Check:   "enabled", // Enable health checks
		Inter:   inter,     // Check interval from backend config
		Rise:    rise,      // Rise threshold from backend config
		Fall:    fall,      // Fall threshold from backend config
	}

	serialized := serializeRuntimeAddServer(ras, haversion)
	if err := runtime.AddServer(backendName, serverName, serialized); err != nil {
		return err
	}

	// Servers are added in MAINT state by default, need to enable them
	if err := runtime.EnableServer(backendName, serverName); err != nil {
		// If enable fails, try to clean up by deleting the server
		_ = runtime.DeleteServer(backendName, serverName)
		return fmt.Errorf("failed to enable server after adding: %w", err)
	}

	return nil
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
// This serializes server parameters for the "add server" runtime command
func serializeRuntimeAddServer(srv *models.RuntimeAddServer, haversion *cn_runtime.HAProxyVersion) string {
	parts := []string{}

	// Address is mandatory and must come first
	addr := srv.Address
	if srv.Port != nil {
		addr = fmt.Sprintf("%s:%d", addr, *srv.Port)
	}
	parts = append(parts, addr)

	// Add health check
	if srv.Check == "enabled" {
		parts = append(parts, "check")
	}

	// Add check interval if specified
	if srv.Inter != nil {
		parts = append(parts, fmt.Sprintf("inter %d", *srv.Inter))
	}

	// Add rise threshold (number of successful checks before UP)
	if srv.Rise != nil {
		parts = append(parts, fmt.Sprintf("rise %d", *srv.Rise))
	}

	// Add fall threshold (number of failed checks before DOWN)
	if srv.Fall != nil {
		parts = append(parts, fmt.Sprintf("fall %d", *srv.Fall))
	}

	// Return space-separated string with leading space
	return " " + strings.Join(parts, " ")
}
