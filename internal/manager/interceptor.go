package manager

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// authHeader is the gRPC metadata key carrying the agent bearer token.
const authHeader = "x-artemis-token"

// peerAddr returns the remote address of the gRPC peer, if known.
func peerAddr(ctx context.Context) (string, bool) {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String(), true
	}
	return "", false
}

// TokenStreamInterceptor returns a stream interceptor that rejects streams whose
// metadata does not carry the expected bearer token. An empty token disables the
// check (open, for local/dev use).
func TokenStreamInterceptor(token string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if token == "" {
			return handler(srv, ss)
		}
		md, ok := metadata.FromIncomingContext(ss.Context())
		if !ok {
			return status.Error(codes.Unauthenticated, "missing metadata")
		}
		vals := md.Get(authHeader)
		if len(vals) == 0 || vals[0] != token {
			return status.Error(codes.Unauthenticated, "invalid agent token")
		}
		return handler(srv, ss)
	}
}

// TokenCredentials returns gRPC per-RPC credentials that attach the agent token.
func TokenCredentials(token string) tokenCreds { return tokenCreds{token: token} }

type tokenCreds struct{ token string }

func (c tokenCreds) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	if c.token == "" {
		return nil, nil
	}
	return map[string]string{authHeader: c.token}, nil
}

func (c tokenCreds) RequireTransportSecurity() bool { return false }
