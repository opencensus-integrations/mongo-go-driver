// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package auth

import (
	"context"

	"github.com/mongodb/mongo-go-driver/core/description"
	"github.com/mongodb/mongo-go-driver/core/wiremessage"

	"go.opencensus.io/trace"
)

func newDefaultAuthenticator(cred *Cred) (Authenticator, error) {
	return &DefaultAuthenticator{
		Cred: cred,
	}, nil
}

// DefaultAuthenticator uses SCRAM-SHA-1 or MONGODB-CR depending
// on the server version.
type DefaultAuthenticator struct {
	Cred *Cred
}

// Auth authenticates the connection.
func (a *DefaultAuthenticator) Auth(ctx context.Context, desc description.Server, rw wiremessage.ReadWriter) error {
	ctx, span := trace.StartSpan(ctx, "mongo-go/core/auth.(*DefaultAuthenticator).Auth")
	defer span.End()

	var actual Authenticator
	var err error
	if err = description.ScramSHA1Supported(desc.Version); err != nil {
		actual, err = newMongoDBCRAuthenticator(a.Cred)
	} else {
		actual, err = newScramSHA1Authenticator(a.Cred)
	}

	if err != nil {
		span.SetStatus(trace.Status{Message: err.Error(), Code: int32(trace.StatusCodeInternal)})
		return err
	}

	return actual.Auth(ctx, desc, rw)
}
