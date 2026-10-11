// mautrix-gmessages - A Matrix-Google Messages puppeting bridge.
// Auto-resend patch (mikesimone fork, 2026-10-11).
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package connector

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

// Messages sent through the bridge can fail on the phone after Google has
// already accepted them (RCS encryption hiccups, carrier errors), which the
// bridge never reports back because the success status already went out.
// Google Messages shows them with a red "!" and a Resend button. This taps
// that button automatically: when a bridge-sent message (IsOutgoing) reaches
// a retryable failed status, wait, re-check that it is still failed, and ask
// the phone to resend it. One delay per attempt; no more after the last.
var autoResendDelays = []time.Duration{30 * time.Second, 2 * time.Minute, 10 * time.Minute}

type autoResendState struct {
	lock     sync.Mutex
	status   map[networkid.MessageID]gmproto.MessageStatusType
	attempts map[networkid.MessageID]int
	pending  map[networkid.MessageID]bool
}

func isRetryableFailure(status gmproto.MessageStatusType) bool {
	switch status {
	case gmproto.MessageStatusType_OUTGOING_FAILED_GENERIC,
		gmproto.MessageStatusType_OUTGOING_FAILED_TO_ENCRYPT,
		gmproto.MessageStatusType_OUTGOING_FAILED_RECIPIENT_LOST_RCS,
		gmproto.MessageStatusType_OUTGOING_FAILED_RECIPIENT_LOST_ENCRYPTION,
		gmproto.MessageStatusType_OUTGOING_FAILED_RECIPIENT_DID_NOT_DECRYPT,
		gmproto.MessageStatusType_OUTGOING_FAILED_RECIPIENT_NEGATIVE_DELIVERY:
		return true
	default:
		return false
	}
}

func (gc *GMClient) noteOutgoingStatus(ctx context.Context, msgID networkid.MessageID, isOutgoing bool, status gmproto.MessageStatusType) {
	if !isOutgoing {
		return
	}
	st := &gc.autoResend
	st.lock.Lock()
	defer st.lock.Unlock()
	if st.status == nil {
		st.status = make(map[networkid.MessageID]gmproto.MessageStatusType)
		st.attempts = make(map[networkid.MessageID]int)
		st.pending = make(map[networkid.MessageID]bool)
	}
	st.status[msgID] = status
	if !isRetryableFailure(status) || st.pending[msgID] {
		return
	}
	n := st.attempts[msgID]
	log := zerolog.Ctx(ctx).With().Str("component", "auto_resend").Str("message_id", string(msgID)).Stringer("status", status).Logger()
	if n >= len(autoResendDelays) {
		log.Warn().Int("attempts", n).Msg("Outgoing message still failed after all auto-resend attempts, giving up")
		return
	}
	st.attempts[msgID] = n + 1
	st.pending[msgID] = true
	log.Info().Int("attempt", n+1).Dur("delay", autoResendDelays[n]).Msg("Outgoing message failed, scheduling auto-resend")
	go gc.autoResendAfter(log.WithContext(context.Background()), msgID, autoResendDelays[n])
}

func (gc *GMClient) autoResendAfter(ctx context.Context, msgID networkid.MessageID, delay time.Duration) {
	log := zerolog.Ctx(ctx)
	time.Sleep(delay)
	st := &gc.autoResend
	st.lock.Lock()
	current := st.status[msgID]
	st.pending[msgID] = false
	st.lock.Unlock()
	if !isRetryableFailure(current) {
		log.Info().Stringer("current_status", current).Msg("Message recovered on its own, not resending")
		return
	}
	rawID, err := gc.ParseMessageID(msgID)
	if err != nil {
		log.Err(err).Msg("Failed to parse message ID for auto-resend")
		return
	}
	if gc.Client == nil {
		log.Warn().Msg("Not connected, skipping auto-resend")
		return
	}
	if err = gc.Client.ResendMessage(ctx, rawID); err != nil {
		log.Err(err).Msg("Auto-resend request failed")
		return
	}
	log.Info().Msg("Asked the phone to resend failed message")
}
