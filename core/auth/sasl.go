// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package auth

import (
	"context"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/core/command"
	"github.com/mongodb/mongo-go-driver/core/description"
	"github.com/mongodb/mongo-go-driver/core/wiremessage"

	"github.com/mongodb/mongo-go-driver/internal/observability"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

// SaslClient is the client piece of a sasl conversation.
type SaslClient interface {
	Start() (string, []byte, error)
	Next(challenge []byte) ([]byte, error)
	Completed() bool
}

// SaslClientCloser is a SaslClient that has resources to clean up.
type SaslClientCloser interface {
	SaslClient
	Close()
}

// ConductSaslConversation handles running a sasl conversation with MongoDB.
func ConductSaslConversation(ctx context.Context, desc description.Server, rw wiremessage.ReadWriter, db string, client SaslClient) error {
	ctx, _ = tag.New(ctx, tag.Insert(observability.KeyMethod, "conduct_sasl_conversation"))
	ctx, span := trace.StartSpan(ctx, "mongo-go/core/auth.ConductSaslConversation")
	defer span.End()

	// Arbiters cannot be authenticated
	if desc.Kind == description.RSArbiter {
		span.Annotatef([]trace.Attribute{
			trace.StringAttribute("arbiter_type", "RSA"),
		}, "Arbiters cannot be authenticated")
		return nil
	}

	if db == "" {
		db = defaultAuthDB
	}

	if closer, ok := client.(SaslClientCloser); ok {
		defer closer.Close()
	}

	mech, payload, err := client.Start()
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "client_start"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return newError(err, mech)
	}

	saslStartCmd := command.Read{
		DB: db,
		Command: bson.NewDocument(
			bson.EC.Int32("saslStart", 1),
			bson.EC.String("mechanism", mech),
			bson.EC.Binary("payload", payload),
		),
	}

	type saslResponse struct {
		ConversationID int    `bson:"conversationId"`
		Code           int    `bson:"code"`
		Done           bool   `bson:"done"`
		Payload        []byte `bson:"payload"`
	}

	var saslResp saslResponse

	ssdesc := description.SelectedServer{Server: desc}
	span.Annotatef(nil, "Invoking saslStartCmd.RoundTrip")
	rdr, err := saslStartCmd.RoundTrip(ctx, ssdesc, rw)
	span.Annotatef(nil, "Finished invoking saslStartCmd.RoundTrip")
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "saslstartcmd_roundtrip"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return newError(err, mech)
	}

	err = bson.Unmarshal(rdr, &saslResp)
	if err != nil {
		ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "unmarshal"))
		stats.Record(ctx, observability.MErrors.M(1))
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return newAuthError("unmarshall error", err)
	}

	cid := saslResp.ConversationID

	for {
		if saslResp.Code != 0 {
			ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "auth"))
			stats.Record(ctx, observability.MErrors.M(1))
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: "Invalid saslResponse"})
			return newError(err, mech)
		}

		if saslResp.Done && client.Completed() {
			return nil
		}

		payload, err = client.Next(saslResp.Payload)
		if err != nil {
			ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "client_next"))
			stats.Record(ctx, observability.MErrors.M(1))
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return newError(err, mech)
		}

		if saslResp.Done && client.Completed() {
			return nil
		}

		saslContinueCmd := command.Read{
			DB: db,
			Command: bson.NewDocument(
				bson.EC.Int32("saslContinue", 1),
				bson.EC.Int32("conversationId", int32(cid)),
				bson.EC.Binary("payload", payload),
			),
		}

		span.Annotatef(nil, "Invoking saslContinueCmd.RoundTrip")
		rdr, err = saslContinueCmd.RoundTrip(ctx, ssdesc, rw)
		span.Annotatef(nil, "Finished invoking saslContinueCmd.RoundTrip")
		if err != nil {
			ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "saslcontinuecmd_roundtrip"))
			stats.Record(ctx, observability.MErrors.M(1))
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return newError(err, mech)
		}

		err = bson.Unmarshal(rdr, &saslResp)
		if err != nil {
			ctx, _ = tag.New(ctx, tag.Upsert(observability.KeyPart, "unmarshal"))
			stats.Record(ctx, observability.MErrors.M(1))
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return newAuthError("unmarshal error", err)
		}
	}
}
