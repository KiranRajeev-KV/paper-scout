package agent

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func pgText(s string) pgtype.Text {
	return pgtype.Text{String: s, Valid: s != ""}
}

func pgTextVal(t pgtype.Text) string {
	if t.Valid {
		return t.String
	}
	return ""
}

func pgFloat64(f float64) pgtype.Float8 {
	return pgtype.Float8{Float64: f, Valid: true}
}

func pgFloat64Val(f pgtype.Float8) float64 {
	if f.Valid {
		return f.Float64
	}
	return 0
}

func pgDate(year int) pgtype.Date {
	if year <= 0 {
		return pgtype.Date{Valid: false}
	}
	return pgtype.Date{
		Time:  time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC),
		Valid: true,
	}
}

func pgDateVal(d pgtype.Date) int {
	if d.Valid {
		return d.Time.Year()
	}
	return 0
}

func parseID(field, value string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid %s %q: %w", field, value, err)
	}
	return id, nil
}

func parseIDs(field string, ids []string) ([]uuid.UUID, error) {
	result := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		u, err := parseID(field, id)
		if err != nil {
			return nil, err
		}
		result = append(result, u)
	}
	return result, nil
}

func truncateText(text string, maxChars int) string {
	runes := []rune(text)
	if len(runes) <= maxChars {
		return text
	}
	return string(runes[:maxChars])
}
