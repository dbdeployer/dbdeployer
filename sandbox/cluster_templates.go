// DBDeployer - The MySQL Sandbox
// Copyright © 2025-2026 Roberto Garcia de Bem
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

//go:build go1.16
// +build go1.16

package sandbox

import (
	_ "embed"

	"github.com/dbdeployer/dbdeployer/globals"
)

// Templates for InnoDB Cluster

var (
	//go:embed templates/cluster/innodb_cluster_options.gotxt
	clusterOptionsTemplate string

	//go:embed templates/cluster/innodb_cluster_options84.gotxt
	clusterOptions84Template string

	//go:embed templates/cluster/init_cluster_nodes.gotxt
	initClusterNodesTemplate string

	//go:embed templates/cluster/check_nodes_cluster.gotxt
	checkClusterNodesTemplate string

	//go:embed templates/cluster/wipe_and_restart_all.gotxt
	clusterWipeAndRestartAllTemplate string

	//go:embed templates/cluster/start_all.gotxt
	clusterStartAllTemplate string

	//go:embed templates/cluster/init_clusterset_nodes.gotxt
	initClusterSetNodesTemplate string

	//go:embed templates/cluster/check_nodes_clusterset.gotxt
	checkClusterSetNodesTemplate string

	//go:embed templates/cluster/wipe_and_restart_all_clusterset.gotxt
	clusterSetWipeAndRestartAllTemplate string

	//go:embed templates/cluster/start_all_clusterset.gotxt
	clusterSetStartAllTemplate string

	//go:embed templates/cluster/start_mysql_router.gotxt
	startMysqlRouterTemplate string

	//go:embed templates/cluster/stop_mysql_router.gotxt
	stopMysqlRouterTemplate string

	//go:embed templates/cluster/status_mysql_router.gotxt
	statusMysqlRouterTemplate string

	//go:embed templates/cluster/mysqlrouter_connections.gotxt
	mysqlrouterConnectionsTemplate string

	//go:embed templates/cluster/router_writer.gotxt
	routerWriterTemplate string

	//go:embed templates/cluster/router_reader.gotxt
	routerReaderTemplate string

	ClusterTemplates = TemplateCollection{
		globals.TmplClusterOptions: TemplateDesc{
			Description: "Set the correct my.cnf configurations",
			Notes:       "",
			Contents:    clusterOptionsTemplate,
		},
		globals.TmplClusterOptions84: TemplateDesc{
			Description: "Set the correct my.cnf configurations for 8.4.x",
			Notes:       "",
			Contents:    clusterOptions84Template,
		},
		globals.TmplInitializeNodesCluster: TemplateDesc{
			Description: "Initialize InnoDB Cluster nodes using MySQL Shell",
			Notes:       "",
			Contents:    initClusterNodesTemplate,
		},
		globals.TmplCheckClusterNodes: TemplateDesc{
			Description: "Checks the status of group replication",
			Notes:       "",
			Contents:    checkClusterNodesTemplate,
		},
		globals.TmplWipeAndRestartAllCluster: TemplateDesc{
			Description: "Wipe and restart all nodes in a InnoDB Cluster",
			Notes:       "",
			Contents:    clusterWipeAndRestartAllTemplate,
		},
		globals.TmplStartAllCluster: TemplateDesc{
			Description: "Start all nodes in a InnoDB Cluster",
			Notes:       "",
			Contents:    clusterStartAllTemplate,
		},
		globals.TmplInitializeNodesClusterSet: TemplateDesc{
			Description: "Initialize InnoDB ClusterSet (HA + DR) using MySQL Shell",
			Notes:       "",
			Contents:    initClusterSetNodesTemplate,
		},
		globals.TmplCheckClusterSetNodes: TemplateDesc{
			Description: "Checks the status of InnoDB ClusterSet",
			Notes:       "",
			Contents:    checkClusterSetNodesTemplate,
		},
		globals.TmplWipeAndRestartAllClusterSet: TemplateDesc{
			Description: "Wipe and restart all nodes in an InnoDB ClusterSet sandbox",
			Notes:       "",
			Contents:    clusterSetWipeAndRestartAllTemplate,
		},
		globals.TmplStartAllClusterSet: TemplateDesc{
			Description: "Start all nodes in an InnoDB ClusterSet sandbox",
			Notes:       "",
			Contents:    clusterSetStartAllTemplate,
		},
		globals.TmplStartMysqlRouter: TemplateDesc{
			Description: "Start MySQL Router (bootstrapped under mysqlrouter/)",
			Notes:       "",
			Contents:    startMysqlRouterTemplate,
		},
		globals.TmplStopMysqlRouter: TemplateDesc{
			Description: "Stop MySQL Router",
			Notes:       "",
			Contents:    stopMysqlRouterTemplate,
		},
		globals.TmplStatusMysqlRouter: TemplateDesc{
			Description: "Show MySQL Router process / pid status",
			Notes:       "",
			Contents:    statusMysqlRouterTemplate,
		},
		globals.TmplMysqlRouterConnections: TemplateDesc{
			Description: "Print Router config listen ports and mysql client hints",
			Notes:       "",
			Contents:    mysqlrouterConnectionsTemplate,
		},
		globals.TmplRouterWriter: TemplateDesc{
			Description: "mysql client or mysqlsh (x) through Router R/W",
			Notes:       "",
			Contents:    routerWriterTemplate,
		},
		globals.TmplRouterReader: TemplateDesc{
			Description: "mysql client or mysqlsh (x) through Router R/O",
			Notes:       "",
			Contents:    routerReaderTemplate,
		},
	}
)
