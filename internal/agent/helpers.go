package agent

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func pgText(s string) pgtype.Text {
	return pgtype.Text{String: s, Valid: s != ""}
}

func pgTextPtr(s *string) pgtype.Text {
	if s == nil || *s == "" {
		return pgtype.Text{Valid: false}
	}
	return pgtype.Text{String: *s, Valid: true}
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

func pgUUID(s string) uuid.UUID {
	id, _ := uuid.Parse(s)
	return id
}

func pgUUIDPtr(s string) pgtype.UUID {
	id, err := uuid.Parse(s)
	if err != nil {
		return pgtype.UUID{Valid: false}
	}
	return pgtype.UUID{Bytes: id, Valid: true}
}

func pgUUIDsFromStrings(ids []string) []uuid.UUID {
	result := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		if u, err := uuid.Parse(id); err == nil {
			result = append(result, u)
		}
	}
	return result
}

func pgBool(b bool) pgtype.Bool {
	return pgtype.Bool{Bool: b, Valid: true}
}

func truncateText(text string, maxChars int) string {
	if len(text) <= maxChars {
		return text
	}
	return text[:maxChars]
}
