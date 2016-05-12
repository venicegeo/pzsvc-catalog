// Copyright 2016, RadiantBlue Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
	"github.com/venicegeo/pzsvc-image-catalog/catalog"
)

const imageCatalogPrefix = "pzsvc-image-catalog"

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of the Harvest CLI",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("harvest v0.1 -- HEAD")
	},
}

// Execute adds all child commands to the root command PlanetCmd and sets flags
// appropriately.
func Execute() {
	var command = &cobra.Command{
		Use: "pzsvc-catalog",
		Long: `
  pzsvc-catalog is a command-line interface for the Beachfront catalog.`,
	}

	var planetKey string
	planetCmd.Flags().StringVar(&planetKey, "PL_API_KEY", "", "Planet Labs API Key")
	catalog.SetPlanetAPIKey(planetKey)

	command.AddCommand(serveCmd)
	command.AddCommand(planetCmd)
	command.AddCommand(versionCmd)

	if err := command.Execute(); err != nil {
		log.Fatal(err)
	}
}
