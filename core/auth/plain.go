// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package auth

import (
	"context"
	"fmt"

	"github.com/mongodb/mongo-go-driver/core/description"
	"github.com/mongodb/mongo-go-driver/core/wiremessage"

	"go.opencensus.io/trace"
)

// PLAIN is the mechanism name for PLAIN.
const PLAIN = "PLAIN"

func newPlainAuthenticator(cred *Cred) (Authenticator, error) {
	if cred.Source != "" && cred.Source != "$external" {
		return nil, fmt.Errorf("PLAIN source must be empty or $external")
	}

	return &PlainAuthenticator{
		Username: cred.Username,
		Password: cred.Password,
	}, nil
}

// PlainAuthenticator uses the PLAIN algorithm over SASL to authenticate a connection.
type PlainAuthenticator struct {
	Username string
	Password string
}

// Auth authenticates the connection.
func (a *PlainAuthenticator) Auth(ctx context.Context, desc description.Server, rw wiremessage.ReadWriter) error {
	ctx, span := trace.StartSpan(ctx, "mongo-go/core/auth/(*PlainAuthenticator).Auth")
	defer span.End()

	err := ConductSaslConversation(ctx, desc, rw, "$external", &plainSaslClient{
		username: a.Username,
		password: a.Password,
	})
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	}
	return err
}

type plainSaslClient struct {
	username string
	password string
}

func (c *plainSaslClient) Start() (string, []byte, error) {
	b := []byte("\x00" + c.username + "\x00" + c.password)
	return PLAIN, b, nil
}

func (c *plainSaslClient) Next(challenge []byte) ([]byte, error) {
	return nil, fmt.Errorf("unexpected server challenge")
}

func (c *plainSaslClient) Completed() bool {
	return true
}
