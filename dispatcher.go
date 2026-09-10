// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otelsarama

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/IBM/sarama"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"
)

type consumerMessagesDispatcher interface {
	Messages() <-chan *sarama.ConsumerMessage
}

type consumerMessagesDispatcherWrapper struct {
	d        consumerMessagesDispatcher
	messages chan *sarama.ConsumerMessage

	// done is closed when the downstream consumer has stopped reading Messages(),
	// releasing Run from a send that would otherwise block forever.
	done      chan struct{}
	closeOnce sync.Once

	cfg config
}

func newConsumerMessagesDispatcherWrapper(d consumerMessagesDispatcher, cfg config) *consumerMessagesDispatcherWrapper {
	return &consumerMessagesDispatcherWrapper{
		d:        d,
		messages: make(chan *sarama.ConsumerMessage),
		done:     make(chan struct{}),
		cfg:      cfg,
	}
}

// Messages returns the read channel for the messages that are returned by
// the broker.
func (w *consumerMessagesDispatcherWrapper) Messages() <-chan *sarama.ConsumerMessage {
	return w.messages
}

// Close signals Run to stop relaying messages. It must be called once the
// downstream consumer will no longer read from Messages(), otherwise Run stays
// blocked on its pending send and never returns. A stranded Run goroutine holds
// a *sarama.ConsumerMessage whose Value aliases the entire decompressed record
// batch it was decoded from, so the whole batch stays reachable and cannot be
// collected.
//
// Close does not wait for Run to return and is safe to call concurrently and
// more than once.
func (w *consumerMessagesDispatcherWrapper) Close() {
	w.closeOnce.Do(func() {
		close(w.done)
	})
}

func (w *consumerMessagesDispatcherWrapper) Run() {
	defer close(w.messages)

	msgs := w.d.Messages()

	for msg := range msgs {
		// Extract a span context from message to link.
		carrier := NewConsumerMessageCarrier(msg)
		parentSpanContext := w.cfg.Propagators.Extract(context.Background(), carrier)

		// Create a span.
		attrs := []attribute.KeyValue{
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(msg.Topic),
			semconv.MessagingOperationTypeReceive,
			semconv.MessagingMessageID(strconv.FormatInt(msg.Offset, 10)),
			semconv.MessagingDestinationPartitionID(strconv.Itoa(int(msg.Partition))),
		}
		opts := []trace.SpanStartOption{
			trace.WithAttributes(attrs...),
			trace.WithSpanKind(trace.SpanKindConsumer),
		}
		newCtx, span := w.cfg.Tracer.Start(parentSpanContext, fmt.Sprintf("%s receive", msg.Topic), opts...)

		// Inject current span context, so consumers can use it to propagate span.
		w.cfg.Propagators.Inject(newCtx, carrier)

		// Send messages back to user, unless the consumer has stopped reading.
		// The escape hatch mirrors what sarama's own relay does on shutdown
		// (partitionConsumer.responseFeeder selects on child.dying); without it a
		// consumer that abandons the channel strands this goroutine forever.
		select {
		case w.messages <- msg:
			span.End()
		case <-w.done:
			span.End()
			return
		}
	}
}
