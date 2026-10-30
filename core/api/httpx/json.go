package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string       `json:"code"`
	Message string       `json:"message"`
	Fields  []FieldError `json:"fields,omitempty"`
}

// DecodeJSON читает один JSON-объект, ограничивает размер body и запрещает лишние поля.
func DecodeJSON(r *http.Request, dst any) error {
	defer r.Body.Close()

	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(dst); err != nil {
		switch {
		case errors.Is(err, io.EOF):
			return ValidationError("request body is required")
		default:
			return ValidationError(fmt.Sprintf("invalid JSON: %v", err))
		}
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return ValidationError("request body must contain a single JSON object")
	}

	return nil
}

// WriteJSON пишет JSON-ответ с указанным статусом.
func WriteJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if payload == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(payload)
}

// WriteNoContent пишет ответ 204 без тела.
func WriteNoContent(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNoContent)
}

// WriteError преобразует StatusError в стандартный JSON-ответ API.
// Неизвестные ошибки маскируются как internal_error.
func WriteError(w http.ResponseWriter, err error) {
	statusErr := GetStatusError(err)
	if statusErr == nil {
		WriteJSON(w, http.StatusInternalServerError, errorEnvelope{
			Error: errorBody{
				Code:    "internal_error",
				Message: "internal server error",
			},
		})
		return
	}

	WriteJSON(w, statusErr.Status, errorEnvelope{
		Error: errorBody{
			Code:    statusErr.Code,
			Message: statusErr.Message,
			Fields:  statusErr.Fields,
		},
	})
}

// PathInt читает path-параметр как положительное целое число.
func PathInt(r *http.Request, name string) (int, error) {
	raw := r.PathValue(name)
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, ValidationError(fmt.Sprintf("%s must be a positive integer", name))
	}
	return value, nil
}
