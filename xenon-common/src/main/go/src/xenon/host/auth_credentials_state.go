package host

import (
	"golang.org/x/net/context"

	"xenon/client"
	"xenon/common"
	"xenon/operation"
	"xenon/uri"
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
