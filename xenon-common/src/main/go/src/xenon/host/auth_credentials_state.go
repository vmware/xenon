// Copyright (c) 2015-2016 VMware, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License.  You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, without warranties or
// conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
// specific language governing permissions and limitations under the License.

package host

import (
	"xenon/client"
	"xenon/common"
	"xenon/operation"
	"xenon/uri"

	"golang.org/x/net/context"
)

const (
	AuthTypePublicKey = "PublicKey"
	AuthTypePassword  = "Password"
)

type AuthCredentialsServiceState struct {
	common.ServiceDocument

	// Client ID.
	UserLink string `json:"userLink,omitempty"`

	// Client email.
	UserEmail string `json:"userEmail,omitempty"`

	// Service Account private key
	PrivateKey string `json:"privateKey,omitempty"`

	// Service Account public key
	PublicKey string `json:"publicKey,omitempty"`

	// Service Account private key id
	PrivateKeyID string `json:"privateKeyId,omitempty"`

	// Token server URI.
	TokenReference string `json:"tokenReference,omitempty"`

	// Type of authentication
	Type string `json:"type,omitempty"`
}

func GetAuthCredentialsServiceState(ctx context.Context, a *AuthCredentialsServiceState, u uri.URI) error {
	op := operation.NewGet(ctx, u)
	err := client.Send(op).Wait()
	if err != nil {
		return err
	}

	if err := op.DecodeBody(a); err != nil {
		return err
	}

	return nil
}
