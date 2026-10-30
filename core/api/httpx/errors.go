// Package httpx предоставляет HTTP-утилиты для BusyBee API:
// чтение/запись JSON, типизированные ошибки со статусами (400/404/409),
// парсинг path-параметров и generic Optional[T] для partial update (PATCH).
package httpx

import "errors"

// FieldError описывает одну ошибку конкретного поля запроса.
type FieldError struct {
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

// StatusError связывает доменную ошибку API с HTTP-статусом и machine-readable кодом.
type StatusError struct {
	Status int
	Code   string

	Message string
	Fields  []FieldError
}

// Error возвращает текстовое описание ошибки.
func (e *StatusError) Error() string {
	return e.Message
}

// ValidationError создает HTTP 400 для некорректного пользовательского ввода.
func ValidationError(message string) error {
	return &StatusError{
		Status:  400,
		Code:    "validation_error",
		Message: message,
	}
}

// ValidationErrors создает HTTP 400 с общим сообщением и деталями по полям.
func ValidationErrors(message string, fields []FieldError) error {
	return &StatusError{
		Status:  400,
		Code:    "validation_error",
		Message: message,
		Fields:  fields,
	}
}

// NotFoundError создает HTTP 404 для отсутствующей сущности или маршрута.
func NotFoundError(message string) error {
	return &StatusError{
		Status:  404,
		Code:    "not_found",
		Message: message,
	}
}

// ConflictError создает HTTP 409 для конфликтов состояния или уникальности.
func ConflictError(message string) error {
	return &StatusError{
		Status:  409,
		Code:    "conflict",
		Message: message,
	}
}

// UnauthorizedError создает HTTP 401 для отсутствующей или неверной авторизации.
func UnauthorizedError(message string) error {
	return &StatusError{
		Status:  401,
		Code:    "unauthorized",
		Message: message,
	}
}

// GetStatusError извлекает StatusError из произвольной цепочки ошибок.
func GetStatusError(err error) *StatusError {
	var target *StatusError
	if errors.As(err, &target) {
		return target
	}
	return nil
}
