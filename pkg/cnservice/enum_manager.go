// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cnservice

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/frontend"
)

const (
	N = 12
)

type EKey [N]byte

// 1. Init enum: lock the enummanager and realse after init.
// 2. Common usage.Just get the map.(drop table or alter table?:)
type EnumManager struct {
	// Long standing bghandler.
	// Getting the specific enum values using att_relname_id from the mo_columns table.
	// This operation doesn't execute frequently.
	bghandler frontend.BackgroundExec

	Mu sync.Mutex

	// enumstoi map[EKey]map[string]uint16
	// enumitos map[EKey][]string
	enumstoi sync.Map
	enumitos sync.Map
}

// Passing.
// Gracefully. solution: runtime service

func (em *EnumManager) GetEnums(tableId uint64, enumId int) (enumstoi map[string]uint16, enumitos []string) {
	k := getKey(tableId, enumId)
	// need a long standing BackgroundHandler.
	if _, ok := em.enumstoi.Load(k); ok {
		return
	} else {
		// Doesn't exist. Init.
		em.Mu.Lock()
		defer em.Mu.Unlock()
		if _, ok = em.enumstoi.Load(k); !ok {
			// Got enum values
			// em.bghandler.
			// store new values.
		}
		// Here value stored.
		return
	}
}

func getKey(tableId uint64, enumId int) (ek EKey) {
	return
}
