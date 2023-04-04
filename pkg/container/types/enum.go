// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !debug
// +build !debug

package types

import "github.com/matrixorigin/matrixone/pkg/common/moerr"

func (a *Enum1) ToString(typ *Type) (string, error) {
	if typ.EnumValues == nil {
		return "", moerr.NewInternalErrorNoCtx("Enum type got no enumvalues")
	}
	index := uint8(*a)
	if index == 0 {
		return "", nil
	}
	if index > uint8(len(typ.EnumValues)) {
		return "", moerr.NewInternalErrorNoCtx("Enum type got values bigger than its length")
	}
	return typ.EnumValues[index-1], nil
}

func (a *Enum2) ToString(typ *Type) (string, error) {
	if typ.EnumValues == nil {
		return "", moerr.NewInternalErrorNoCtx("Enum type got no enumvalues")
	}
	index := uint16(*a)
	if index == 0 {
		return "", nil
	}
	if index > uint16(len(typ.EnumValues)) {
		return "", moerr.NewInternalErrorNoCtx("Enum type got values bigger than its length")
	}
	return typ.EnumValues[index-1], nil
}
