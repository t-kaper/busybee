package api

import (
	"busybee/core/api/httpx"
	"busybee/core/common"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

const dbTimeout = 5 * time.Second

// ============================================================================
// StartHTTPServer — поднимает HTTP API и останавливает его вместе с ctx
// ============================================================================
// Функция создает корневой router, оборачивает его в единый middleware
// обработки паник и HTTP-ошибок и запускает graceful shutdown по отмене ctx.
func StartHTTPServer(ctx context.Context, addr string, bearerToken string, pool *pgxpool.Pool) error {
	api := New(pool)

	server := &http.Server{
		Addr:              addr,
		Handler:           withHTTPErrorHandling(withBearerAuth(api.Handler(), bearerToken)),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			common.ZapLogger.Error("HTTP server shutdown failed", zap.Error(err))
		}
	}()

	common.ZapLogger.Info("HTTP server started", zap.String("addr", addr))
	err := server.ListenAndServe()

	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	common.ZapLogger.Info("HTTP server stopped", zap.String("addr", addr))
	return nil
}

// withBearerAuth защищает все `/api/*` маршруты статическим Bearer-токеном.
func withBearerAuth(next http.Handler, expectedToken string) http.Handler {
	expectedToken = strings.TrimSpace(expectedToken)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/") {
			next.ServeHTTP(w, r)
			return
		}

		authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
		if !strings.HasPrefix(authHeader, "Bearer ") {
			w.Header().Set("WWW-Authenticate", "Bearer")
			httpx.WriteError(w, httpx.UnauthorizedError("missing or invalid bearer token"))
			return
		}

		providedToken := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
		// Сравниваем фиксированные SHA-256 дайджесты, чтобы не оставлять различия
		// по времени ответа из-за разной длины исходных токенов.
		expectedHash := sha256.Sum256([]byte(expectedToken))
		providedHash := sha256.Sum256([]byte(providedToken))
		if subtle.ConstantTimeCompare(providedHash[:], expectedHash[:]) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			httpx.WriteError(w, httpx.UnauthorizedError("missing or invalid bearer token"))
			return
		}

		next.ServeHTTP(w, r)
	})
}

// withHTTPErrorHandling нормализует неожиданные HTTP-сценарии до единого JSON-формата.
// В частности, здесь перехватываются panic, 404 и 405 от стандартного mux.
func withHTTPErrorHandling(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := newBufferedResponseWriter(w)

		defer func() {
			if recovered := recover(); recovered != nil {
				common.ZapLogger.Error(
					"HTTP handler panic",
					zap.Any("panic", recovered),
					zap.String("method", r.Method),
					zap.String("path", r.URL.Path),
					zap.ByteString("stack", debug.Stack()),
				)
				httpx.WriteError(w, errors.New("internal server error"))
			}
		}()

		next.ServeHTTP(recorder, r)

		switch recorder.status {
		case http.StatusNotFound:
			common.ZapLogger.Warn(
				"HTTP route not found",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
			)
			httpx.WriteError(w, httpx.NotFoundError("route not found"))
		case http.StatusMethodNotAllowed:
			if allow := recorder.header.Get("Allow"); allow != "" {
				w.Header().Set("Allow", allow)
			}
			httpx.WriteError(w, &httpx.StatusError{
				Status:  http.StatusMethodNotAllowed,
				Code:    "method_not_allowed",
				Message: "method not allowed",
			})
		default:
			recorder.Flush()
		}
	})
}

// bufferedResponseWriter временно буферизует статус, заголовки и body,
// чтобы middleware мог решить, как именно завершить ответ.
type bufferedResponseWriter struct {
	original    http.ResponseWriter
	header      http.Header
	body        bytes.Buffer
	status      int
	wroteHeader bool
}

// newBufferedResponseWriter создает буферизующий writer поверх исходного ResponseWriter.
func newBufferedResponseWriter(w http.ResponseWriter) *bufferedResponseWriter {
	return &bufferedResponseWriter{
		original: w,
		header:   make(http.Header),
	}
}

// Header возвращает локальную карту заголовков буферизованного ответа.
func (w *bufferedResponseWriter) Header() http.Header {
	return w.header
}

// WriteHeader сохраняет статус до финального Flush.
func (w *bufferedResponseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.status = status
	w.wroteHeader = true
}

// Write записывает body во внутренний буфер.
func (w *bufferedResponseWriter) Write(data []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.body.Write(data)
}

// Flush копирует накопленные заголовки, статус и body в исходный writer.
func (w *bufferedResponseWriter) Flush() {
	for key, values := range w.header {
		for _, value := range values {
			w.original.Header().Add(key, value)
		}
	}

	status := w.status
	if status == 0 {
		status = http.StatusOK
	}

	w.original.WriteHeader(status)
	if w.body.Len() == 0 {
		return
	}
	_, _ = w.original.Write(w.body.Bytes())
}
