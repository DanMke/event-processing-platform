package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DanMke/event-processing-platform/internal/config"
	"github.com/DanMke/event-processing-platform/internal/domain"
	"github.com/DanMke/event-processing-platform/internal/messaging/kafka"
	"github.com/DanMke/event-processing-platform/internal/observability"
)

type eventJob struct {
	key   []byte
	value []byte
	kind  string
}

func main() {
	observability.Init()

	cfg := config.LoadProducer()

	total := envInt("TOTAL_EVENTS", 1000)
	tenants := envInt("TENANTS", 5)
	invalidRatio := envFloat64("INVALID_RATIO", 0.05)
	dupRatio := envFloat64("DUPLICATE_RATIO", 0.02)
	concurrency := envInt("CONCURRENCY", 4)

	slog.Info("loadgen starting",
		"total_events", total,
		"tenants", tenants,
		"invalid_ratio", invalidRatio,
		"duplicate_ratio", dupRatio,
		"concurrency", concurrency,
		"brokers", cfg.Brokers,
		"topic", cfg.Topic,
	)

	jobs := generate(total, tenants, invalidRatio, dupRatio)

	p := kafka.NewProducer(cfg.Brokers, cfg.Topic)
	defer p.Close()

	start := time.Now()

	var (
		sentValid     int64
		sentInvalid   int64
		sentDuplicate int64
		errCount      int64
	)

	ch := make(chan eventJob, concurrency*2)
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range ch {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err := p.PublishRaw(ctx, job.key, job.value)
				cancel()
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					slog.Warn("publish failed", "error_reason", err.Error())
					continue
				}
				switch job.kind {
				case "valid":
					atomic.AddInt64(&sentValid, 1)
				case "invalid":
					atomic.AddInt64(&sentInvalid, 1)
				case "duplicate":
					atomic.AddInt64(&sentDuplicate, 1)
				}
			}
		}()
	}

	for _, j := range jobs {
		ch <- j
	}
	close(ch)
	wg.Wait()

	elapsed := time.Since(start)
	totalSent := sentValid + sentInvalid + sentDuplicate
	throughput := float64(totalSent) / elapsed.Seconds()

	slog.Info("loadgen finished",
		"total_sent", totalSent,
		"valid", sentValid,
		"invalid", sentInvalid,
		"duplicates", sentDuplicate,
		"errors", errCount,
		"duration_ms", elapsed.Milliseconds(),
		"throughput_rps", fmt.Sprintf("%.0f", throughput),
	)
}

func generate(total, tenants int, invalidRatio, dupRatio float64) []eventJob {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	invalidCount := int(float64(total) * invalidRatio)
	dupCount := int(float64(total) * dupRatio)
	validCount := total - invalidCount - dupCount
	if validCount < 0 {
		validCount = 0
	}

	now := time.Now().UTC()
	jobs := make([]eventJob, 0, total)

	type validEntry struct {
		idx int
		eid string
	}
	validPool := make([]validEntry, 0, validCount)

	for i := 0; i < validCount; i++ {
		tid := tenantID(i, tenants)
		eid := fmt.Sprintf("evt-%d-%06d", now.Unix(), i)
		validPool = append(validPool, validEntry{i, eid})
		b, _ := json.Marshal(buildValidEvent(eid, tid, i, now))
		jobs = append(jobs, eventJob{key: []byte(tid), value: b, kind: "valid"})
	}

	for i := 0; i < invalidCount; i++ {
		tid := tenantID(i, tenants)
		k, v := buildInvalidEvent(i, tid, now)
		jobs = append(jobs, eventJob{key: k, value: v, kind: "invalid"})
	}

	for i := 0; i < dupCount && len(validPool) > 0; i++ {
		orig := validPool[rng.Intn(len(validPool))]
		tid := tenantID(orig.idx, tenants)
		b, _ := json.Marshal(buildValidEvent(orig.eid, tid, orig.idx, now))
		jobs = append(jobs, eventJob{key: []byte(tid), value: b, kind: "duplicate"})
	}

	rng.Shuffle(len(jobs), func(i, j int) { jobs[i], jobs[j] = jobs[j], jobs[i] })
	return jobs
}

func buildValidEvent(eid, tid string, idx int, now time.Time) domain.Event {
	base := domain.Event{
		EventID: eid, TenantID: tid, SchemaVersion: "1.0",
		OccurredAt: now, Producer: "loadgen",
		TraceID: fmt.Sprintf("trace-%d", idx),
	}
	switch idx % 5 {
	case 0:
		p, _ := json.Marshal(map[string]any{
			"contract_id": fmt.Sprintf("contract-%d", idx),
			"amount":      float64((idx + 1) * 100),
			"currency":    "BRL",
		})
		base.EventType, base.Payload = "contract.created", json.RawMessage(p)
	case 1:
		p, _ := json.Marshal(map[string]any{
			"contract_id": fmt.Sprintf("contract-%d", idx),
			"reason":      "customer_request",
		})
		base.EventType, base.Payload = "contract.cancelled", json.RawMessage(p)
	case 2:
		p, _ := json.Marshal(map[string]any{
			"transaction_id": fmt.Sprintf("txn-%d", idx),
			"amount":         float64((idx + 1) * 50),
			"currency":       "BRL",
			"merchant_id":    fmt.Sprintf("merchant-%d", idx%10),
			"card_last_four": fmt.Sprintf("%04d", idx%10000),
		})
		base.EventType, base.Payload = "payment.processed", json.RawMessage(p)
	case 3:
		reasons := []string{"insufficient_funds", "card_blocked", "expired_card", "fraud_detected"}
		p, _ := json.Marshal(map[string]any{
			"transaction_id": fmt.Sprintf("txn-fail-%d", idx),
			"amount":         float64((idx + 1) * 75),
			"currency":       "BRL",
			"merchant_id":    fmt.Sprintf("merchant-%d", idx%10),
			"failure_reason": reasons[idx%len(reasons)],
		})
		base.EventType, base.Payload = "payment.failed", json.RawMessage(p)
	default:
		types := []string{"checking", "savings", "credit"}
		p, _ := json.Marshal(map[string]any{
			"account_id":   fmt.Sprintf("acc-%d", idx),
			"account_type": types[idx%len(types)],
			"owner_id":     fmt.Sprintf("owner-%d", idx%100),
		})
		base.EventType, base.Payload = "account.created", json.RawMessage(p)
	}
	return base
}

func buildInvalidEvent(idx int, tid string, now time.Time) (key, value []byte) {
	switch idx % 4 {
	case 0:
		return []byte(tid), []byte(`{not-valid-json}`)
	case 1:
		v, _ := json.Marshal(map[string]any{
			"event_id":   fmt.Sprintf("evt-inv-%d", idx),
			"event_type": "contract.created",
		})
		return []byte(tid), v
	case 2:
		p, _ := json.Marshal(map[string]any{
			"contract_id": "c-invalid",
			"amount":      "not-a-number",
			"currency":    "BRL",
		})
		v, _ := json.Marshal(domain.Event{
			EventID: fmt.Sprintf("evt-inv-%d", idx), TenantID: tid,
			EventType: "contract.created", SchemaVersion: "1.0",
			OccurredAt: now, Producer: "loadgen",
			Payload: json.RawMessage(p),
		})
		return []byte(tid), v
	default:
		p, _ := json.Marshal(map[string]any{"data": "unknown"})
		v, _ := json.Marshal(domain.Event{
			EventID: fmt.Sprintf("evt-inv-%d", idx), TenantID: tid,
			EventType: "unknown.event.type", SchemaVersion: "1.0",
			OccurredAt: now, Producer: "loadgen",
			Payload: json.RawMessage(p),
		})
		return []byte(tid), v
	}
}

func tenantID(i, tenants int) string {
	return fmt.Sprintf("tenant-%02d", i%tenants)
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat64(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
