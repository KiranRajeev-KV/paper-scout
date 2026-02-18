package logger

import (
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

var log zerolog.Logger

func init() {
	log = zerolog.New(os.Stdout).With().Timestamp().Logger()
}

func SetDevelopment() {
	file, err := os.OpenFile("dev.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		file = os.Stdout
	}

	console := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.Kitchen,
		NoColor:    false,
	}

	multi := io.MultiWriter(console, file)
	log = zerolog.New(multi).With().Timestamp().Logger()
	log = log.With().Caller().Logger()
}

func SetProduction() {
	file, err := os.OpenFile("prod.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		file = os.Stdout
	}

	log = zerolog.New(file).With().Timestamp().Logger()
}

func SetLevel(level string) {
	switch level {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
}

func SetOutput(w io.Writer) {
	log = log.Output(w)
}

func Get() *zerolog.Logger {
	return &log
}

func Debug() *zerolog.Event {
	return log.Debug()
}

func Info() *zerolog.Event {
	return log.Info()
}

func Warn() *zerolog.Event {
	return log.Warn()
}

func Error() *zerolog.Event {
	return log.Error()
}

func Fatal() *zerolog.Event {
	return log.Fatal()
}

func Panic() *zerolog.Event {
	return log.Panic()
}

func With() zerolog.Context {
	return log.With()
}

func Err(err error) *zerolog.Event {
	return log.Err(err)
}
