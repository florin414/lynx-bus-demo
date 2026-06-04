package main

import (
	"bytes"
	"io"
	"log"
	"os"
	"strings"
)

const (
	ansiReset = "\033[0m"

	// Timestamp
	colorTimestamp = "\033[90m" // dark gray

	// Message body colors
	colorMsgNormal = "\033[37m"   // light gray        — default
	colorMsgBold   = "\033[1;97m" // bold bright white  — step banners
	colorMsgDim    = "\033[90m"   // dark gray          — config / cleanup
	colorMsgError  = "\033[91m"   // bright red         — errors / fatal

	// Component bracket colors  (matched by component name)
	colorCompStep       = "\033[1;93m" // bold bright yellow  — STEP N headers
	colorCompDemo       = "\033[93m"   // bright yellow       — demo sub-sections
	colorCompBroker     = "\033[94m"   // bright blue
	colorCompMain       = "\033[97m"   // bright white
	colorCompProducer   = "\033[92m"   // bright green
	colorCompConsumer   = "\033[96m"   // bright cyan
	colorCompObserver   = "\033[35m"   // magenta
	colorCompClassifier = "\033[95m"   // bright magenta
	colorCompStorage    = "\033[33m"   // yellow / amber
	colorCompConfig     = "\033[90m"   // dark gray
	colorCompError      = "\033[91m"   // bright red
	colorCompDeadline   = "\033[91m"   // bright red         — deadline demo
	colorCompStream     = "\033[95m"   // bright magenta     — stream processing
	colorCompEvent      = "\033[36m"   // cyan               — event sourcing / commit log
	colorCompCleanup    = "\033[90m"   // dark gray          — cleanup
	colorCompDefault    = "\033[36m"   // cyan               — fallback
)

func setupLogging() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetOutput(newColorLogWriter(os.Stderr, colorsEnabled()))
}

func colorsEnabled() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	term := strings.TrimSpace(os.Getenv("TERM"))
	return term != "" && term != "dumb"
}

type colorLogWriter struct {
	out     io.Writer
	enabled bool
}

func newColorLogWriter(out io.Writer, enabled bool) io.Writer {
	return &colorLogWriter{out: out, enabled: enabled}
}

func (w *colorLogWriter) Write(p []byte) (int, error) {
	if !w.enabled {
		return w.out.Write(p)
	}

	var b strings.Builder
	data := p
	for len(data) > 0 {
		if i := bytes.IndexByte(data, '\n'); i >= 0 {
			line := strings.TrimSuffix(string(data[:i]), "\r")
			b.WriteString(formatColoredLogLine(line))
			b.WriteByte('\n')
			data = data[i+1:]
			continue
		}
		b.WriteString(formatColoredLogLine(strings.TrimSuffix(string(data), "\r")))
		break
	}

	return w.out.Write([]byte(b.String()))
}

func formatColoredLogLine(line string) string {
	if line == "" {
		return line
	}

	space := strings.IndexByte(line, ' ')
	if space <= 0 {
		return colorize(colorMsgNormal, line)
	}

	timestamp := line[:space]
	rest := line[space+1:]
	if strings.TrimSpace(rest) == "" {
		return colorize(colorTimestamp, timestamp) + " " + rest
	}

	leadingSpaces := rest[:len(rest)-len(strings.TrimLeft(rest, " "))]
	trimmed := strings.TrimLeft(rest, " ")

	if component, message, ok := splitComponent(trimmed); ok {
		inner := strings.ToLower(strings.Trim(component, "[]"))
		compColor := pickComponentColor(inner)
		msgColor := pickMessageColor(inner)

		formatted := colorize(colorTimestamp, timestamp) + " " + leadingSpaces + colorize(compColor, component)
		if message != "" {
			formatted += " " + colorize(msgColor, message)
		}
		return formatted
	}

	return colorize(colorTimestamp, timestamp) + " " + colorize(colorMsgNormal, rest)
}

func pickComponentColor(inner string) string {
	switch inner {
	case "step":
		return colorCompStep
	case "broker":
		return colorCompBroker
	case "main":
		return colorCompMain
	case "producer", "publisher":
		return colorCompProducer
	case "consumer":
		return colorCompConsumer
	case "observer":
		return colorCompObserver
	case "classifier":
		return colorCompClassifier
	case "storage":
		return colorCompStorage
	case "config":
		return colorCompConfig
	case "error":
		return colorCompError
	case "fatal":
		return colorCompError
	case "demo":
		return colorCompDemo
	}
	if strings.Contains(inner, "deadline") {
		return colorCompDeadline
	}
	if strings.Contains(inner, "cleanup") {
		return colorCompCleanup
	}
	if strings.Contains(inner, "eventsourcing") || strings.Contains(inner, "commitlog") || strings.Contains(inner, "snapshot") {
		return colorCompEvent
	}
	if strings.Contains(inner, "stream") {
		return colorCompStream
	}
	if strings.HasPrefix(inner, "demo-") {
		return colorCompDemo
	}
	if strings.Contains(inner, "consumer") || strings.Contains(inner, "aggregator") {
		return colorCompConsumer
	}
	if strings.Contains(inner, "producer") || strings.Contains(inner, "publisher") {
		return colorCompProducer
	}
	return colorCompDefault
}

func pickMessageColor(inner string) string {
	switch {
	case inner == "step":
		return colorMsgBold
	case inner == "error", inner == "fatal":
		return colorMsgError
	case inner == "config" || strings.Contains(inner, "cleanup"):
		return colorMsgDim
	default:
		return colorMsgNormal
	}
}

func splitComponent(line string) (component, message string, ok bool) {
	if !strings.HasPrefix(line, "[") {
		return "", "", false
	}
	end := strings.IndexByte(line, ']')
	if end <= 0 {
		return "", "", false
	}

	component = line[:end+1]
	message = strings.TrimLeft(line[end+1:], " ")
	return component, message, true
}

func colorize(code, text string) string {
	return code + text + ansiReset
}
