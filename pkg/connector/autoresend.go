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
	"strings"
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
// that button automatically: when one of our outgoing messages reaches a
// retryable failed status within autoResendMaxAge of being sent, wait,
// re-check that it is still failed, and ask the phone to resend it. One delay
// per attempt; no more after the last.
//
// Not limited to IsOutgoing (bridge-sent) messages: the bridge often fails to
// match its own sends to the phone's echo, and stores them as if typed on the
// phone without that flag. Outgoing statuses only ever belong to our side, and
// the age cap keeps old failures that resurface in a resync from being resent.
var autoResendDelays = []time.Duration{30 * time.Second, 2 * time.Minute, 10 * time.Minute}

const autoResendMaxAge = time.Hour

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

func (gc *GMClient) noteOutgoingStatus(ctx context.Context, msgID networkid.MessageID, sentAt time.Time, status gmproto.MessageStatusType) {
	if !strings.HasPrefix(status.String(), "OUTGOING_") {
		return
	}
	if isRetryableFailure(status) && time.Since(sentAt) > autoResendMaxAge {
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
	if isSuccessfullySentStatus(status) && !st.pending[msgID] {
		// Delivered: forget it so the maps stay small.
		delete(st.status, msgID)
		delete(st.attempts, msgID)
		return
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
