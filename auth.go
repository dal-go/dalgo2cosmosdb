package dalgo2cosmosdb

import (
	"context"
	"strings"
)

// Credential has no exported secret fields and always redacts formatting. Use
// ResourceToken or AADToken; zero credentials are invalid. Providers should use
// least-privilege read authority. This client never creates accounts or keys.
type Credential struct{ header string }

func (Credential) String() string   { return "cosmos credential [redacted]" }
func (Credential) GoString() string { return "cosmos credential [redacted]" }

type AuthRequest struct{ Method, Resource, Scope string }
type AuthorizationProvider func(context.Context, AuthRequest) (Credential, error)

// ResourceToken takes an UNENCODED broker-issued resource token. The closed
// signature alphabet makes this exact percent encoder equivalent to the
// official JS SDK's encodeURIComponent branch, including + / =. Encoded input,
// extra fields, master keys and control characters are rejected, never guessed.
func ResourceToken(raw string) (Credential, error) {
	const prefix = "type=resource&ver=1.0&sig="
	if !strings.HasPrefix(raw, prefix) || len(raw) > 12*1024 {
		return Credential{}, ErrConfiguration
	}
	sig := strings.TrimPrefix(raw, prefix)
	if sig == "" {
		return Credential{}, ErrConfiguration
	}
	for _, b := range []byte(sig) {
		if !((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || strings.ContainsRune("+/=_-", rune(b))) {
			return Credential{}, ErrConfiguration
		}
	}
	const hex = "0123456789ABCDEF"
	var out strings.Builder
	for _, b := range []byte(raw) {
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || strings.ContainsRune("-_.!~*'()", rune(b)) {
			out.WriteByte(b)
		} else {
			out.WriteByte('%')
			out.WriteByte(hex[b>>4])
			out.WriteByte(hex[b&15])
		}
	}
	return Credential{header: out.String()}, nil
}

// AADToken takes the raw broker/credential access token, not a Bearer header or
// a Cosmos authorization string. The raw type=aad format matches the official
// Go azcosmos bearer policy. Token audience/RBAC remains the caller's concern.
func AADToken(raw string) (Credential, error) {
	if raw == "" || len(raw) > 12*1024 {
		return Credential{}, ErrConfiguration
	}
	for _, b := range []byte(raw) {
		if !((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || strings.ContainsRune("-._~+/=", rune(b))) {
			return Credential{}, ErrConfiguration
		}
	}
	return Credential{header: "type=aad&ver=1.0&sig=" + raw}, nil
}
