package app

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type metrics struct {
	registry      *prometheus.Registry
	webhooks      *prometheus.CounterVec
	threads       *prometheus.CounterVec
	probes        *prometheus.CounterVec
	enqueues      *prometheus.CounterVec
	deliveries    *prometheus.CounterVec
	pending       prometheus.Gauge
	oldestPending prometheus.Gauge
}

func newMetrics() *metrics {
	m := &metrics{registry: prometheus.NewRegistry()}
	m.webhooks = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "noor_gateway_webhook_requests_total", Help: "Gateway webhook requests by result."}, []string{"result"})
	m.threads = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "noor_thread_events_total", Help: "Gateway-authorized thread events by board, kind, and result."}, []string{"board", "kind", "result"})
	m.probes = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "noor_stream_probes_total", Help: "Stream probes by result."}, []string{"result"})
	m.enqueues = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "noor_notification_enqueues_total", Help: "Notification queue attempts by source and result."}, []string{"source", "result"})
	m.deliveries = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "noor_notification_delivery_attempts_total", Help: "Telegram notification delivery attempts by result."}, []string{"result"})
	m.pending = prometheus.NewGauge(prometheus.GaugeOpts{Name: "noor_notification_pending", Help: "Pending notification deliveries."})
	m.oldestPending = prometheus.NewGauge(prometheus.GaugeOpts{Name: "noor_notification_oldest_pending_seconds", Help: "Age of the oldest pending notification."})
	m.registry.MustRegister(m.webhooks, m.threads, m.probes, m.enqueues, m.deliveries, m.pending, m.oldestPending, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	return m
}

func (m *metrics) handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
func (m *metrics) webhook(result string) { m.webhooks.WithLabelValues(result).Inc() }
func (m *metrics) thread(board, kind, result string) {
	m.threads.WithLabelValues(board, kind, result).Inc()
}
func (m *metrics) ObserveStreamProbe(result string) { m.probes.WithLabelValues(result).Inc() }
func (m *metrics) ObserveNotificationEnqueue(source, result string) {
	m.enqueues.WithLabelValues(source, result).Inc()
}
func (m *metrics) ObserveNotificationDelivery(result string) {
	m.deliveries.WithLabelValues(result).Inc()
}
func (m *metrics) SetPendingNotifications(count int, age time.Duration) {
	m.pending.Set(float64(count))
	m.oldestPending.Set(age.Seconds())
}
